package repo

import (
	"context"
	"net/http"
)

type Repository interface {
	Get(bucket string, key string) (string, error)
	GetAll(bucket string) ([][]byte, error)
	Update(bucket string, key string, value string) error
	Delete(bucket string, key string) error
	DeleteBucket(bucket string) error
	InitSchema(buckets []string) error
	Close() error
	Backup(ctx context.Context, res http.ResponseWriter) error
	NextSequence(bucket string) (uint64, error)
	Query(namespace string, opts QueryOptions) (int, [][]byte)
}

var impl Repository

func SetRepository(repository Repository) {
	impl = repository
}

func Get(bucket string, key string) (string, error) {
	return impl.Get(bucket, key)
}

func GetAll(bucket string) ([][]byte, error) {
	return impl.GetAll(bucket)
}

func Update(bucket string, key string, value string) error {
	return impl.Update(bucket, key, value)
}

func InitSchema(buckets []string) error {
	return impl.InitSchema(buckets)
}

func Delete(bucket string, key string) error {
	return impl.Delete(bucket, key)
}

func Backup(ctx context.Context, res http.ResponseWriter) error {
	return impl.Backup(ctx, res)
}

func NextSequence(bucket string) (uint64, error) {
	return impl.NextSequence(bucket)
}

func Query(namespace string, opts QueryOptions) (int, [][]byte) {
	return impl.Query(namespace, opts)
}

func Close() error {
	return impl.Close()
}

func DeleteBucket(bucket string) error {
	return impl.DeleteBucket(bucket)
}
