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
	"github.com/minio/minio/pkg/bucket/replication"
	"github.com/minio/minio/pkg/madmin"
)

// BucketTargetSys represents replication subsystem
type BucketTargetSys struct {
	sync.RWMutex
	targetsMap    map[string]*miniogo.Core
	targetsARNMap map[string]string
}

// GetConfig - gets replication config associated to a given bucket name.
func (sys *BucketTargetSys) GetConfig(ctx context.Context, bucketName string) (rc *replication.Config, err error) {
	if globalIsGateway {
		objAPI := newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		return nil, BucketTargetConfigNotFound{Bucket: bucketName}
	}

	return globalBucketTargetSys.GetConfig(ctx, bucketName)
}

// SetTarget - sets a new minio-go client replication target for this bucket.
func (sys *BucketTargetSys) SetTarget(ctx context.Context, bucket string, tgt *madmin.BucketTarget) error {
	if globalIsGateway {
		return nil
	}
	// delete replication targets that were removed
	if tgt.Empty() {
		sys.Lock()
		if currTgt, ok := sys.targetsMap[bucket]; ok {
			delete(sys.targetsARNMap, currTgt.EndpointURL().String())
		}
		delete(sys.targetsMap, bucket)
		sys.Unlock()
		return nil
	}
	clnt, err := getBucketTargetClient(tgt)
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
		targetsMap:    make(map[string]*miniogo.Core),
		targetsARNMap: make(map[string]string),
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
		tgt, err := globalBucketMetadataSys.GetBucketTargetConfig(bucket.Name)
		if err != nil {
			continue
		}
		if tgt == nil || tgt.Empty() {
			continue
		}
		tgtClient, err := getBucketTargetClient(tgt)
		if err != nil {
			continue
		}
		sys.Lock()
		sys.targetsMap[bucket.Name] = tgtClient
		sys.targetsARNMap[tgt.URL()] = tgt.Arn
		sys.Unlock()
	}
}

// GetARN returns the ARN associated with replication target URL
func (sys *BucketTargetSys) getARN(endpoint string) string {
	return sys.targetsARNMap[endpoint]
}

// getBucketTargetInstanceTransport contains a singleton roundtripper.
var getBucketTargetInstanceTransport http.RoundTripper
var getBucketTargetInstanceTransportOnce sync.Once

// Returns a minio-go Client configured to access remote host described in replication target config.
var getBucketTargetClient = func(tcfg *madmin.BucketTarget) (*miniogo.Core, error) {
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
		Transport: getReplicationTargetInstanceTransport,
	})
	return core, err
}

// getARN gets existing ARN for an endpoint or generates a new one.
func (sys *BucketTargetSys) getTargetARN(endpoint string) string {
	arn, ok := sys.targetsARNMap[endpoint]
	if ok {
		return arn
	}
	return fmt.Sprintf("arn:minio:s3::%s:*", mustGetUUID())
}
