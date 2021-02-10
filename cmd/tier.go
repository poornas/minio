/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
)

// TierConfigPath refers to remote tier config object name
var TierConfigPath string = path.Join(minioConfigPrefix, "tier-config.json")

var (
	errTierInsufficientCreds = errors.New("insufficient tier credentials supplied")
	errWarmBackendInUse      = errors.New("Backend warm tier already in use")
	errTierTypeUnsupported   = errors.New("Unsupported tier type")
)

// TierConfigMgr holds the collection of remote tiers configured in this deployment.
type TierConfigMgr struct {
	sync.RWMutex
	drivercache map[string]warmBackend
	Tiers       map[string]madmin.TierConfig `json:"tiers"`
}

// isTierNameInUse returns tier type and true if there exists a remote tier by
// name tierName, otherwise returns madmin.Unsupported and false.
func (config *TierConfigMgr) isTierNameInUse(tierName string) (madmin.TierType, bool) {
	if t, ok := config.Tiers[tierName]; ok {
		return t.Type, true
	}
	return madmin.Unsupported, false
}

// Add adds tier to config if it passes all validations.
func (config *TierConfigMgr) Add(tier madmin.TierConfig) error {
	config.Lock()
	defer config.Unlock()

	// check if tier name is in all caps
	tierName := tier.Name
	if tierName != strings.ToUpper(tierName) {
		return errTierNameNotUppercase
	}

	// check if tier name already in use
	if _, exists := config.isTierNameInUse(tierName); exists {
		return errTierAlreadyExists
	}

	d, err := newWarmBackend(context.TODO(), tier)
	if err != nil {
		return err
	}
	// Check if warmbackend is in use by other MinIO tenants
	inUse, err := d.InUse(context.TODO())
	if err != nil {
		return err
	}
	if inUse {
		return errWarmBackendInUse
	}

	config.Tiers[tierName] = tier
	config.drivercache[tierName] = d

	return nil
}

// ListTiers lists remote tiers configured in this deployment.
func (config *TierConfigMgr) ListTiers() []madmin.TierConfig {
	config.RLock()
	defer config.RUnlock()

	var tierCfgs []madmin.TierConfig
	for _, tier := range config.Tiers {
		// This makes a local copy of tier config before
		// passing a reference to it.
		tier := tier
		tierCfgs = append(tierCfgs, tier)
	}
	return tierCfgs
}

// Edit replaces the credentials of the remote tier specified by tierName with creds.
func (config *TierConfigMgr) Edit(tierName string, creds madmin.TierCreds) error {
	config.Lock()
	defer config.Unlock()

	// check if tier by this name exists
	tierType, exists := config.isTierNameInUse(tierName)
	if !exists {
		return errTierNotFound
	}

	newCfg := config.Tiers[tierName]
	switch tierType {
	case madmin.S3:
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierInsufficientCreds
		}
		newCfg.S3.AccessKey = creds.AccessKey
		newCfg.S3.SecretKey = creds.SecretKey

	case madmin.Azure:
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierInsufficientCreds
		}
		newCfg.Azure.AccountName = creds.AccessKey
		newCfg.Azure.AccountKey = creds.SecretKey
	case madmin.GCS:
		if creds.CredsJSON == nil {
			return errTierInsufficientCreds
		}
		newCfg.GCS.Creds = base64.URLEncoding.EncodeToString(creds.CredsJSON)
	}

	d, err := newWarmBackend(context.TODO(), newCfg)
	if err != nil {
		return err
	}
	config.Tiers[tierName] = newCfg
	config.drivercache[tierName] = d
	return nil
}

// Bytes returns the json encoded bytes of config
func (config *TierConfigMgr) Bytes() ([]byte, error) {
	config.Lock()
	defer config.Unlock()
	return json.Marshal(config)
}

// GetDriver returns a warmbackend interface object initialized with remote tier config matching tierName
func (config *TierConfigMgr) GetDriver(tierName string) (d warmBackend, err error) {
	config.Lock()
	defer config.Unlock()

	var ok bool
	// Lookup in-memory drivercache
	d, ok = config.drivercache[tierName]
	if ok {
		return d, nil
	}

	// Initialize driver from tier config matching tierName
	t, ok := config.Tiers[tierName]
	if !ok {
		return nil, errTierNotFound
	}
	d, err = newWarmBackend(context.TODO(), t)
	if err != nil {
		return nil, err
	}
	config.drivercache[tierName] = d
	return d, nil
}

// configReader returns a PutObjReader and ObjectOptions needed to save config
// using a PutObject API. PutObjReader encrypts json encoded tier configurations
// if KMS is enabled, otherwise simply yields the json encoded bytes as is.
// Similarly, ObjectOptions value depends on KMS' status.
func (config *TierConfigMgr) configReader() (*PutObjReader, *ObjectOptions, error) {
	b, err := config.Bytes()
	if err != nil {
		return nil, nil, err
	}

	payloadSize := int64(len(b))
	br := bytes.NewReader(b)
	hr, err := hash.NewReader(br, payloadSize, "", "", payloadSize, false)
	if err != nil {
		return nil, nil, err
	}
	if GlobalKMS == nil {
		return NewPutObjReader(hr), &ObjectOptions{}, nil
	}

	// Note: Local variables with names ek, oek, etc are named inline with
	// acronyms defined here -
	// https://github.com/minio/minio/blob/master/docs/security/README.md#acronyms

	// Encrypt json encoded tier configurations
	metadata := make(map[string]string)
	sseS3 := true
	var extKey [32]byte
	encBr, oek, err := newEncryptReader(hr, extKey[:], minioMetaBucket, TierConfigPath, metadata, sseS3)
	if err != nil {
		return nil, nil, err
	}

	info := ObjectInfo{
		Size: payloadSize,
	}
	encSize := info.EncryptedSize()
	encHr, err := hash.NewReader(encBr, encSize, "", "", encSize, false)
	if err != nil {
		return nil, nil, err
	}
	pReader, err := NewPutObjReader(hr).WithEncryption(encHr, &oek)
	if err != nil {
		return nil, nil, err
	}
	opts := &ObjectOptions{
		UserDefined: metadata,
		MTime:       UTCNow(),
	}
	return pReader, opts, nil
}

func saveGlobalTierConfig() error {
	pr, opts, err := globalTierConfigMgr.configReader()
	if err != nil {
		return err
	}

	_, err = globalObjectAPI.PutObject(context.Background(), minioMetaBucket, TierConfigPath, pr, *opts)
	return err
}

func loadGlobalTransitionTierConfig() error {
	objReadCloser, err := globalObjectAPI.GetObjectNInfo(context.Background(), minioMetaBucket, TierConfigPath, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		if isErrObjectNotFound(err) {
			globalTierConfigMgr = &TierConfigMgr{
				RWMutex:     sync.RWMutex{},
				drivercache: make(map[string]warmBackend),
				Tiers:       make(map[string]madmin.TierConfig),
			}
			return nil
		}
		return err
	}

	defer objReadCloser.Close()

	var config TierConfigMgr
	err = json.NewDecoder(objReadCloser).Decode(&config)
	if err != nil {
		return err
	}
	if config.drivercache == nil {
		config.drivercache = make(map[string]warmBackend)
	}
	if config.Tiers == nil {
		config.Tiers = make(map[string]madmin.TierConfig)
	}

	globalTierConfigMgr = &config
	return nil
}
