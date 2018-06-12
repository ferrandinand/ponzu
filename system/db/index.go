package db

import (
	"encoding/json"

	"github.com/ponzu-cms/ponzu/system/db/repo"
)

// Index gets the value from the namespace at the key provided
func Index(namespace, key string) ([]byte, error) {

	v, err := repo.Get(index(namespace), key)
	if err != nil {
		return nil, err
	}

	return []byte(v), nil
}

// SetIndex sets a key/value pair within the namespace provided and will return
// an error if it fails
func SetIndex(namespace, key string, value interface{}) error {

	val, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return repo.Update(index(namespace), key, string(val))
}

// DeleteIndex removes the key and value from the namespace provided and will
// return an error if it fails. It will return nil if there was no key/value in
// the index to delete.
func DeleteIndex(namespace, key string) error {

	return repo.Delete(index(namespace), key)

}

// DropIndex removes the index and all key/value pairs in the namespace index
func DropIndex(namespace string) error {
	return repo.DeleteBucket(index(namespace))
}

func index(namespace string) string {
	return "__index_" + namespace
}
