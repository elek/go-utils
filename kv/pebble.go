package kv

import (
	"bytes"
	"github.com/cockroachdb/pebble"
	"io"
	"strings"
	"time"
)

type Pebble struct {
	db *pebble.DB

}

func CreatePebble(dir string) (*Pebble, error) {
	db, err := pebble.Open(dir, &pebble.Options{
		ErrorIfNotExists: false,
	})
	if err != nil {
		return &Pebble{}, err
	}
	return &Pebble{
		db: db,
	}, nil
}

func (pb *Pebble) Put(key string, value []byte) error {
	return pb.db.Set([]byte(key), value, &pebble.WriteOptions{
		Sync: false,
	})
}

func (pb *Pebble) List(prefix string) ([]string, error) {
	result := make([]string, 0)
	it := pb.db.NewIter(nil)
	it.SeekGE([]byte(prefix))
	for {
		key := string(it.Key())
		if !strings.HasPrefix(key, prefix) {
			break
		}
		rel := key[len(prefix)+1:]
		parts := strings.Split(rel, "/")
		if len(parts) > 1 {
			subdir := prefix + "/" + parts[0]
			result = append(result, subdir)
			for {
				it.Next()
				nextKey := string(it.Key())
				if !strings.HasPrefix(nextKey, subdir) {
					break
				}
			}
		} else {
			result = append(result, key)
			if !it.Next() {
				break
			}
		}

	}
	return result, nil
}

func (pb *Pebble) Contains(key string) bool {
	return false
}

func (pb *Pebble) Get(prefix string) ([]byte, error) {
	data, _, err := pb.db.Get([]byte(prefix))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (pb *Pebble) GetReader(prefix string) (io.Reader, error) {
	content, err := pb.Get(prefix)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(content), nil
}

func (pb *Pebble) GetOrDefault(key string, defaultFunc Getter) ([]byte, error) {
	if !pb.Contains(key) {
		val, err := defaultFunc(key)
		if err != nil {
			return nil, err
		}
		pb.Put(key, val)
	}
	return pb.Get(key)
}

func (pb *Pebble) Iterate(prefix string, action IteratorAction) error {
	it := pb.db.NewIter(nil)
	it.SeekGE([]byte(prefix))
	for {
		key := string(it.Key())
		if !strings.HasPrefix(key, prefix) {
			break
		}
		rel := key[len(prefix)+1:]
		parts := strings.Split(rel, "/")
		if len(parts) > 1 {
			subdir := prefix + "/" + parts[0]
			err := action(subdir)
			if err != nil {
				return err
			}
			for {
				it.Next()
				nextKey := string(it.Key())
				if !strings.HasPrefix(nextKey, subdir) {
					break
				}
			}
		} else {
			err := action(key)
			if err != nil {
				return err
			}
			if !it.Next() {
				break
			}
		}

	}
	return nil
}

func (pb *Pebble) IterateAll(action IteratorAction) error {
	it := pb.db.NewIter(nil)
	it.First()
	for {
		key := string(it.Key())
		err := action(key)
		if err != nil {
			return err
		}
		if !it.Next() {
			break
		}
	}
	return nil
}
func (pb *Pebble) IsChanged(since time.Time, prefix string) (bool, error) {
	return true, nil
}
func (pb *Pebble) IterateSubTree(prefix string, action IteratorAction) error {
	options := pebble.IterOptions{LowerBound: []byte(prefix)}
	it := pb.db.NewIter(&options)
	it.First()
	for {
		key := string(it.Key())
		err := action(key)
		if err != nil {
			return err
		}
		if !it.Next() {
			break
		}
	}
	return nil
}

func (pb *Pebble) Close() error {
	return nil
}