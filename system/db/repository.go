package repo

type Repository interface {
	Get(bucket string, key string) (string, error)
	GetAll(bucket string) ([][]byte, error)
	Update(bucket string, key string, value string) error
	Delete(bucket string, key string) error
	InitSchema(buckets []string) error
	Close()
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
