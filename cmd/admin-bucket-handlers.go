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
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/env"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

const (
	bucketQuotaConfigFile = "quota.json"
	bucketTargetsFile     = "bucket-targets.json"
)

// PutBucketQuotaConfigHandler - PUT Bucket quota configuration.
// ----------
// Places a quota configuration on the specified bucket. The quota
// specified in the quota configuration will be applied by default
// to enforce total quota for the specified bucket.
func (a adminAPIHandlers) PutBucketQuotaConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketQuotaConfig")

	defer logger.AuditLog(w, r, "PutBucketQuotaConfig", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.SetBucketQuotaAdminAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Turn off quota commands if data usage info is unavailable.
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOff {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminBucketQuotaDisabled), r.URL)
		return
	}

	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if _, err = parseBucketQuota(bucket, data); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if err = globalBucketMetadataSys.Update(bucket, bucketQuotaConfigFile, data); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketQuotaConfigHandler - gets bucket quota configuration
func (a adminAPIHandlers) GetBucketQuotaConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketQuotaConfig")

	defer logger.AuditLog(w, r, "GetBucketQuotaConfig", mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.GetBucketQuotaAdminAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	config, err := globalBucketMetadataSys.GetQuotaConfig(bucket)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	configData, err := json.Marshal(config)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseJSON(w, configData)
}

// SetBucketTargetHandler - sets a remote target for bucket
func (a adminAPIHandlers) SetBucketTargetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetBucketTarget")

	defer logger.AuditLog(w, r, "SetBucketTarget", mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Get current object layer instance.
	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.SetBucketTargetAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	cred, _, _, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	password := cred.SecretKey

	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	var target madmin.BucketTarget
	if err = json.Unmarshal(reqBytes, &target); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	target.Arn = globalBucketTargetSys.getTargetARN(target)
	targets := madmin.BucketTargets{
		Targets: make([]madmin.BucketTarget, 1),
	}
	targets.Targets[0] = target
	tgtBytes, err := json.Marshal(&targets)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	if err = globalBucketMetadataSys.Update(bucket, bucketTargetsFile, tgtBytes); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	if err = globalBucketTargetSys.SetTarget(ctx, bucket, &target); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// RemoveBucketTargetHandler - removes a remote target for bucket of a particular type
func (a adminAPIHandlers) RemoveBucketTargetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveBucketTarget")

	defer logger.AuditLog(w, r, "RemoveBucketTarget", mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	arnType := vars["type"]

	// Get current object layer instance.
	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.SetBucketTargetAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	if !globalIsErasure {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	tgtBytes, err := json.Marshal(&madmin.BucketTarget{})
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}
	if err = globalBucketMetadataSys.Update(bucket, bucketTargetsFile, tgtBytes); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	if err = globalBucketTargetSys.RemoveTarget(ctx, bucket, arnType); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketTargetsHandler - gets bucket  targets for a particular bucket
func (a adminAPIHandlers) GetBucketTargetsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketTarget")

	defer logger.AuditLog(w, r, "GetBucketTarget", mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	// Get current object layer instance.
	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.GetBucketTargetAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cfg, err := globalBucketMetadataSys.GetBucketTargetConfig(bucket)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	var target madmin.BucketTarget
	if !cfg.Empty() {
		target = cfg.Targets[0]
	}
	// remove secretKey from creds
	var tgt madmin.BucketTarget
	if !target.Empty() {
		var creds auth.Credentials
		creds.AccessKey = target.Credentials.AccessKey
		tgt = madmin.BucketTarget{Endpoint: target.Endpoint, TargetBucket: target.TargetBucket, Credentials: &creds, Arn: target.Arn}
	}
	data, err := json.Marshal(tgt)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	// Write success response.
	writeSuccessResponseJSON(w, data)
}

// ListBucketTargetARNHandler - gets target ARN for a particular remote
func (a adminAPIHandlers) ListBucketTargetARNHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketReplicationARN")

	defer logger.AuditLog(w, r, "GetBucketReplicationARN", mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	rURL := vars["url"]
	rType := vars["type"]
	// Get current object layer instance.
	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.GetBucketTargetAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	data, err := json.Marshal(globalBucketTargetSys.listARN(rURL, rType))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	// Write success response.
	writeSuccessResponseJSON(w, data)
}
