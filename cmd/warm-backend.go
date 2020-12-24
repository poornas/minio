package cmd

import (
	"io"
	"net/url"

	minio "github.com/minio/minio-go"
)

type warmBackend interface {
	Put(bucket, object string, r io.Reader, length int64) error
	Get(bucket, object string) (io.ReadCloser, int64, error)
	Remove(bucket, object string) error
}

type warmBackendMinio struct {
	client *minio.Client
}

func (w *warmBackendMinio) Put(bucket, object string, r io.Reader, length int64) error {
	_, err := w.client.PutObject(bucket, object, r, length, minio.PutObjectOptions{})
	return err
}

func (w *warmBackendMinio) Get(bucket, object string) (r io.ReadCloser, err error) {
	return w.client.GetObject(bucket, object, minio.GetObjectOptions{})
}

func (w *warmBackendMinio) Remove(bucket, object string) error {
	return w.client.RemoveObject(bucket, object)
}

func newWarmBackend(endpoint, accessKey, secretKey string) (*warmBackendMinio, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(u.Host, accessKey, secretKey, u.Scheme == "https")
	if err != nil {
		return nil, err
	}
	return &warmBackendMinio{client}, nil
}
