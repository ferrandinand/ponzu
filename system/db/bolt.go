package db

import (
	"fmt"
	"log"

	"github.com/boltdb/bolt"
	"github.com/ferrandinand/ponzu/system/item"
)

type BoltRepository struct {
	store *bolt.DB
}

func NewBolt() (*BoltRepository, error) {

	fmt.Printf("Starting new db")
	store, err := bolt.Open("system2.db", 0666, nil)

	if err != nil {
		log.Fatalln(err)
	}

	return &BoltRepository{
		store,
	}, nil
}

func (r *BoltRepository) InitSchema(buckets []string) error {

	err := r.store.Update(func(tx *bolt.Tx) error {

		// initialize db with all content type buckets & sorted bucket for type
		for t := range item.Types {
			_, err := tx.CreateBucketIfNotExists([]byte(t))
			if err != nil {
				return err
			}

			_, err = tx.CreateBucketIfNotExists([]byte(t + "__sorted"))
			if err != nil {
				return err
			}
		}

		// init db with other buckets as needed
		for _, name := range buckets {
			_, err := tx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.Fatalln("Coudn't initialize db with buckets.", err)
	}

	return nil

}

func (r *BoltRepository) Close() {
	err := r.store.Close()
	if err != nil {
		log.Println(err)
	}
}

func (r *BoltRepository) Get(bucket string, key string) (string, error) {

	var val []byte

	err := r.store.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		val = b.Get([]byte(key))

		return nil

	})

	if err != nil {
		return "", err
	}

	return string(val), nil
}

func (r *BoltRepository) Update(bucket string, k string, v string) error {

	err := r.store.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		err := b.Put([]byte(k), []byte(v))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *BoltRepository) GetAll(bucket string) ([][]byte, error) {

	var values [][]byte

	r.store.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		b.ForEach(func(k, v []byte) error {
			values = append(values, v)

			return nil
		})

		return nil
	})

	return values, nil
}

func (r *BoltRepository) Delete(bucket string, key string) error {
	err := r.store.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		if err := b.Delete([]byte(key)); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
