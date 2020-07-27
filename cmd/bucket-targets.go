/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/madmin"
)

// BucketTargetSys represents bucket target subsystem
type BucketTargetSys struct {
	sync.RWMutex
	clientsCache  map[string]*miniogo.Core
	targetsMap    map[string]map[string]*miniogo.Core
	targetsARNMap map[string]string
}

// SetTarget - sets a new remote target for bucket.
func (sys *BucketTargetSys) SetTarget(ctx context.Context, bucket string, tgt *madmin.BucketTarget) error {
	if globalIsGateway {
		return nil
	}
	clnt, err := sys.getBucketTargetClient(tgt)
	if err != nil {
		return BucketTargetNotFound{Bucket: tgt.TargetBucket}
	}
	ok, err := clnt.BucketExists(ctx, tgt.TargetBucket)
	if err != nil {
		return err
	}
	if !ok {
		return BucketTargetDestinationNotFound{Bucket: tgt.TargetBucket}
	}
	sys.Lock()
	sys.targetsMap[bucket] = clnt
	sys.targetsARNMap[tgt.URL()] = tgt.Arn
	sys.Unlock()
	return nil
}

// RemoveTarget - removes a remote bucket target for this source bucket.
func (sys *BucketTargetSys) RemoveTarget(ctx context.Context, bucket, arnType string) error {
	if globalIsGateway {
		return nil
	}
	// delete bucket target of specific arnType that was removed
	sys.Lock()
	if currTgt, ok := sys.targetsMap[bucket]; ok {
		delete(sys.targetsARNMap, fmt.Sprintf("%s%s", arnType, currTgt.EndpointURL().String()))
	}
	delete(sys.targetsMap, bucket)
	sys.Unlock()
	return nil
}

// GetTargetClient returns minio-go client for target instance
func (sys *BucketTargetSys) GetTargetClient(ctx context.Context, bucket string) *miniogo.Core {
	var clnt *miniogo.Core
	sys.RLock()
	if c, ok := sys.targetsMap[bucket]; ok {
		clnt = c
	}
	sys.RUnlock()
	return clnt
}

// NewBucketTargetSys - creates new replication system.
func NewBucketTargetSys() *BucketTargetSys {
	return &BucketTargetSys{
		targetsMap:    make(map[string]map[string]*miniogo.Core),
		targetsARNMap: make(map[string]string),
		clientsCache:  make(map[string]*miniogo.Core),
	}
}

// Init initializes the bucket replication subsystem for buckets with replication config
func (sys *BucketTargetSys) Init(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, replication is not supported.
	if globalIsGateway {
		return nil
	}

	// Load bucket replication targets once during boot.
	sys.load(ctx, buckets, objAPI)
	return nil
}

// create minio-go clients for buckets having replication targets
func (sys *BucketTargetSys) load(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) {
	for _, bucket := range buckets {
		cfg, err := globalBucketMetadataSys.GetBucketTargetConfig(bucket.Name)
		if err != nil {
			continue
		}
		if cfg == nil || cfg.Empty() {
			continue
		}
		// for now handle only replication target
		tgt := cfg.Targets[0]
		tgtClient, err := sys.getBucketTargetClient(&tgt)
		if err != nil {
			continue
		}
		sys.Lock()
		if sys.targetsMap[bucket.Name] == nil {
			sys.targetsMap[bucket.Name] = make(map[string]*miniogo.Core)
		}
		bucketClients := sys.targetsMap[bucket.Name]
		bucketClients[tgt.URL()] = tgtClient
		sys.targetsMap[bucket.Name] = bucketClients
		if _, ok := sys.clientsCache[tgt.URL()]; !ok {
			sys.clientsCache[tgt.URL()] = tgtClient
		}
		sys.targetsARNMap[fmt.Sprintf("%s,%s", tgt.Type, tgt.URL())] = tgt.Arn
		sys.Unlock()
	}
}

// listARN returns the ARN(s) associated with target URL. If arnType is specified,
// listing is specific to the arn type.
func (sys *BucketTargetSys) listARN(endpoint, arnType string) []string {
	var arns []string
	for k, v := range sys.targetsARNMap {
		if strings.HasSuffix(k, fmt.Sprintf("%s,%s", arnType, endpoint)) {
			arns = append(arns, v)
		}
	}
	return arns
}

// getARN returns the ARN associated with replication target URL
func (sys *BucketTargetSys) getARN(endpoint, arnType string) string {
	return sys.targetsARNMap[fmt.Sprintf("%s,%s", arnType, endpoint)]
}

// getBucketTargetInstanceTransport contains a singleton roundtripper.
var getBucketTargetInstanceTransport http.RoundTripper
var getBucketTargetInstanceTransportOnce sync.Once

// Returns a minio-go Client configured to access remote host described in replication target config.
func (sys *BucketTargetSys) getBucketTargetClient(tcfg *madmin.BucketTarget) (*miniogo.Core, error) {
	if tcfg == nil {
		return nil, nil
	}
	sys.RLock()
	defer sys.RUnlock()
	if clnt, ok := sys.clientsCache[tcfg.URL()]; ok {
		return clnt, nil
	}
	config := tcfg.Credentials
	// if Signature version '4' use NewV4 directly.
	creds := credentials.NewStaticV4(config.AccessKey, config.SecretKey, "")
	// if Signature version '2' use NewV2 directly.
	if strings.ToUpper(tcfg.API) == "S3V2" {
		creds = credentials.NewStaticV2(config.AccessKey, config.SecretKey, "")
	}

	getBucketTargetInstanceTransportOnce.Do(func() {
		getBucketTargetInstanceTransport = NewGatewayHTTPTransport()
	})
	core, err := miniogo.NewCore(tcfg.Endpoint, &miniogo.Options{
		Creds:     creds,
		Secure:    tcfg.IsSSL,
		Transport: getBucketTargetInstanceTransport,
	})
	return core, err
}

// getARN gets existing ARN for an endpoint or generates a new one.
func (sys *BucketTargetSys) getTargetARN(t madmin.BucketTarget) string {
	key := fmt.Sprintf("%s%s", t.URL(), string(t.Type))
	arn, ok := sys.targetsARNMap[key]
	if ok {
		return arn
	}
	arnStr := ""
	switch t.Type {
	case madmin.Replication:
		arnStr = "s3"
	default:
		arnStr = string(t.Type)
	}
	return fmt.Sprintf("arn:minio:%s::%s:*", arnStr, mustGetUUID())
}
