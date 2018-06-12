package repo

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/boltdb/bolt"
	"github.com/ponzu-cms/ponzu/system/item"
)

type BoltRepository struct {
	store *bolt.DB
}

func NewBolt() (*BoltRepository, error) {

	fmt.Printf("Starting new db")
	store, err := bolt.Open("system.db", 0666, nil)

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

func (r *BoltRepository) Close() error {
	err := r.store.Close()
	if err != nil {
		return err
	}
	return nil
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
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		err = b.Put([]byte(k), []byte(v))
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

func (r *BoltRepository) DeleteBucket(bucket string) error {
	return r.store.Update(func(tx *bolt.Tx) error {

		err := tx.DeleteBucket([]byte(bucket))
		if err == bolt.ErrBucketNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return nil
	})
}

func (r *BoltRepository) Backup(ctx context.Context, res http.ResponseWriter) error {
	errChan := make(chan error, 1)

	go func() {
		errChan <- r.store.View(func(tx *bolt.Tx) error {
			ts := time.Now().Unix()
			disposition := `attachment; filename="system-%d.db.bak"`

			res.Header().Set("Content-Type", "application/octet-stream")
			res.Header().Set("Content-Disposition", fmt.Sprintf(disposition, ts))
			res.Header().Set("Content-Length", fmt.Sprintf("%d", int(tx.Size())))

			_, err := tx.WriteTo(res)
			return err
		})
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

func (r *BoltRepository) NextSequence(bucket string) (uint64, error) {

	var id uint64
	var err error
	err = r.store.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		id, err = b.NextSequence()
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return 0, err
	}
	return id, nil

}

// QueryOptions holds options for a query
type QueryOptions struct {
	Count  int
	Offset int
	Order  string
}

// Query retrieves a set of content from the db based on options
// and returns the total number of content in the namespace and the content
func (r *BoltRepository) Query(namespace string, opts QueryOptions) (int, [][]byte) {
	var posts [][]byte
	var total int

	// correct bad input rather than return nil or error
	// similar to default case for opts.Order switch below
	if opts.Count < 0 {
		opts.Count = -1
	}

	if opts.Offset < 0 {
		opts.Offset = 0
	}

	r.store.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return bolt.ErrBucketNotFound
		}

		c := b.Cursor()
		n := b.Stats().KeyN
		total = n

		// return nil if no content
		if n == 0 {
			return nil
		}

		var start, end int
		switch opts.Count {
		case -1:
			start = 0
			end = n

		default:
			start = opts.Count * opts.Offset
			end = start + opts.Count
		}

		// bounds check on posts given the start & end count
		if start > n {
			start = n - opts.Count
		}
		if end > n {
			end = n
		}

		i := 0   // count of num posts added
		cur := 0 // count of num cursor moves
		switch opts.Order {
		case "desc", "":
			for k, v := c.Last(); k != nil; k, v = c.Prev() {
				if cur < start {
					cur++
					continue
				}

				if cur >= end {
					break
				}

				posts = append(posts, v)
				i++
				cur++
			}

		case "asc":
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if cur < start {
					cur++
					continue
				}

				if cur >= end {
					break
				}

				posts = append(posts, v)
				i++
				cur++
			}

		default:
			// results for DESC order
			for k, v := c.Last(); k != nil; k, v = c.Prev() {
				if cur < start {
					cur++
					continue
				}

				if cur >= end {
					break
				}

				posts = append(posts, v)
				i++
				cur++
			}
		}

		return nil
	})

	return total, posts
}
