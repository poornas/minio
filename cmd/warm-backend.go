package cmd

import (
	"context"
	"io"

	"github.com/minio/minio/pkg/madmin"
)

type warmBackendGetOpts struct {
	startOffset int64
	length      int64
}

type warmBackend interface {
	Put(ctx context.Context, object string, r io.Reader, length int64) error
	Get(ctx context.Context, object string, opts warmBackendGetOpts) (io.ReadCloser, error)
	Remove(ctx context.Context, object string) error
	InUse(ctx context.Context) (bool, error)
	// GetTarget() (string, string)
}

// checkWarmBackend checks if tier config credentials have sufficient privileges
// to perform all operations definde in the warmBackend interface.
// FIXME: currently, we check only for Get.
func checkWarmBackend(ctx context.Context, w warmBackend) error {
	// TODO: requires additional checks to ensure that warmBackend
	// configuration has sufficient privileges to Put/Remove objects as well.
	_, err := w.Get(ctx, "probeobject", warmBackendGetOpts{})
	switch {
	case isErrObjectNotFound(err):
		return nil
	case isErrBucketNotFound(err):
		return errTierBucketNotFound
	default:
		return err
	}
	return nil
}

// newWarmBackend instantiates the tier type specific warmBackend, runs
// checkWarmBackend on it.
func newWarmBackend(ctx context.Context, tier madmin.TierConfig) (d warmBackend, err error) {
	switch tier.Type {
	case madmin.S3:
		d, err = newWarmBackendS3(*tier.S3)
	case madmin.Azure:
		d, err = newWarmBackendAzure(*tier.Azure)
	case madmin.GCS:
		d, err = newWarmBackendGCS(*tier.GCS)
	default:
		return nil, errTierTypeUnsupported
	}
	if err != nil {
		return nil, errTierTypeUnsupported
	}

	err = checkWarmBackend(ctx, d)
	if err != nil {
		return nil, err
	}
	return d, nil
}
