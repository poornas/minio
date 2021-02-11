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

var TierConfigPath string = path.Join(minioConfigPrefix, "tier-config.json")

var (
	errTierInsufficientCreds = errors.New("insufficient tier credentials supplied")
	errWarmBackendInUse      = errors.New("Backend warm tier already in use")
	errTierTypeUnsupported   = errors.New("Unsupported tier type")
)

type TierConfigMgr struct {
	sync.RWMutex
	drivercache map[string]warmBackend
	S3          map[string]madmin.TierS3    `json:"s3"`
	Azure       map[string]madmin.TierAzure `json:"azure"`
	GCS         map[string]madmin.TierGCS   `json:"gcs"`
}

func (config *TierConfigMgr) isTierNameInUse(tierName string) (madmin.TierType, bool) {
	for name := range config.S3 {
		if tierName == name {
			return madmin.S3, true
		}
	}

	for name := range config.Azure {
		if tierName == name {
			return madmin.Azure, true
		}
	}

	for name := range config.GCS {
		if tierName == name {
			return madmin.GCS, true
		}
	}

	return madmin.Unsupported, false
}

func (config *TierConfigMgr) Add(tier madmin.TierConfig) error {
	config.Lock()
	defer config.Unlock()

	// check if tier name is in all caps
	tierName := tier.Name()
	if tierName != strings.ToUpper(tierName) {
		return errTierNameNotUppercase
	}

	// check if tier name already in use
	if _, exists := config.isTierNameInUse(tierName); exists {
		return errTierAlreadyExists
	}

	switch tier.Type {
	case madmin.S3:
		d, err := newWarmBackendS3(*tier.S3)
		if err != nil {
			return err
		}
		err = checkWarmBackend(context.TODO(), d)
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

		config.S3[tierName] = *tier.S3
		config.drivercache[tierName] = d

	case madmin.Azure:
		d, err := newWarmBackendAzure(*tier.Azure)
		if err != nil {
			return err
		}
		err = checkWarmBackend(context.TODO(), d)
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

		config.Azure[tierName] = *tier.Azure
		config.drivercache[tierName] = d

	case madmin.GCS:
		d, err := newWarmBackendGCS(*tier.GCS)
		if err != nil {
			return err
		}
		err = checkWarmBackend(context.TODO(), d)
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

		config.GCS[tierName] = *tier.GCS
		config.drivercache[tierName] = d

	default:
		return errTierTypeUnsupported
	}

	return nil
}

func (config *TierConfigMgr) ListTiers() []madmin.TierConfig {
	config.RLock()
	defer config.RUnlock()

	var configs []madmin.TierConfig
	for _, t := range config.S3 {
		// This makes a local copy of tier config before
		// passing a reference to it.
		cfg := t
		configs = append(configs, madmin.TierConfig{
			Type: madmin.S3,
			S3:   &cfg,
		})
	}

	for _, t := range config.Azure {
		// This makes a local copy of tier config before
		// passing a reference to it.
		cfg := t
		configs = append(configs, madmin.TierConfig{
			Type:  madmin.Azure,
			Azure: &cfg,
		})
	}

	for _, t := range config.GCS {
		// This makes a local copy of tier config before
		// passing a reference to it.
		cfg := t
		configs = append(configs, madmin.TierConfig{
			Type: madmin.GCS,
			GCS:  &cfg,
		})
	}

	return configs
}

func (config *TierConfigMgr) Edit(tierName string, creds madmin.TierCreds) error {
	config.Lock()
	defer config.Unlock()

	// check if tier by this name exists
	tierType, exists := config.isTierNameInUse(tierName)
	if !exists {
		return errTierNotFound
	}

	switch tierType {
	case madmin.S3:
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierInsufficientCreds
		}
		newCfg := config.S3[tierName]
		newCfg.AccessKey = creds.AccessKey
		newCfg.SecretKey = creds.SecretKey
		d, err := newWarmBackendS3(newCfg)
		if err != nil {
			return err
		}
		err = checkWarmBackend(context.TODO(), d)
		if err != nil {
			return err
		}
		config.S3[tierName] = newCfg
		config.drivercache[tierName] = d

	case madmin.Azure:
		if creds.AccessKey == "" || creds.SecretKey == "" {
			return errTierInsufficientCreds
		}
		newCfg := config.Azure[tierName]
		newCfg.AccountName = creds.AccessKey
		newCfg.AccountKey = creds.SecretKey
		d, err := newWarmBackendAzure(newCfg)
		if err != nil {
			return err
		}
		err = checkWarmBackend(context.TODO(), d)
		if err != nil {
			return err
		}
		config.Azure[tierName] = newCfg
		config.drivercache[tierName] = d
	case madmin.GCS:
		if creds.CredsJSON == nil {
			return errTierInsufficientCreds
		}
		newCfg := config.GCS[tierName]
		newCfg.Creds = base64.URLEncoding.EncodeToString(creds.CredsJSON)
		d, err := newWarmBackendGCS(newCfg)
		if err != nil {
			return err
		}
		err = checkWarmBackend(context.TODO(), d)
		if err != nil {
			return err
		}
		config.GCS[tierName] = newCfg
		config.drivercache[tierName] = d
	}

	return nil
}

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

	// Intialize driver from tier config matching tierName
	if s3, ok := config.S3[tierName]; ok {
		d, err = newWarmBackendS3(s3)
		if err != nil {
			return d, err
		}
	} else if az, ok := config.Azure[tierName]; ok {
		d, err = newWarmBackendAzure(az)
		if err != nil {
			return d, err
		}
	} else if gcs, ok := config.GCS[tierName]; ok {
		d, err = newWarmBackendGCS(gcs)
		if err != nil {
			return d, err
		}
	} else { // No matching driver config found
		return nil, errTierNotFound
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
		return NewPutObjReader(hr, nil, nil), &ObjectOptions{}, nil
	}

	// Note: Local variables with names ek, oek, etc are named inline with
	// acronyms defined here -
	// https://github.com/minio/minio/blob/master/docs/security/README.md#acronyms

	// Encrypt json encoded tier configurations
	metadata := make(map[string]string)
	sseS3 := true
	var extKey [32]byte
	encBr, oek, err := newEncryptReader(br, extKey[:], minioMetaBucket, TierConfigPath, metadata, sseS3)
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
	opts := &ObjectOptions{
		UserDefined: metadata,
		MTime:       UTCNow(),
	}
	return NewPutObjReader(hr, encHr, &oek), opts, nil
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
				S3:          make(map[string]madmin.TierS3),
				Azure:       make(map[string]madmin.TierAzure),
				GCS:         make(map[string]madmin.TierGCS),
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
	if config.S3 == nil {
		config.S3 = make(map[string]madmin.TierS3)
	}
	if config.Azure == nil {
		config.Azure = make(map[string]madmin.TierAzure)
	}
	if config.GCS == nil {
		config.GCS = make(map[string]madmin.TierGCS)
	}

	globalTierConfigMgr = &config
	return nil
}
