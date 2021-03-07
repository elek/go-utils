package kv

import "strings"
import "time"
import "io"
import "github.com/pkg/errors"

type KV interface {
	Put(key string, value []byte) error
	List(prefix string) ([]string, error)
	IterateAll(action IteratorAction) error
	Iterate(prefix string, action IteratorAction) error
	IterateSubTree(prefix string, action IteratorAction) error
	Contains(key string) bool
	GetOrDefault(key string, defaultFunc Getter) ([]byte, error)
	Get(prefix string) ([]byte, error)
	GetReader(prefix string) (io.Reader, error)
	IsChanged(since time.Time, prefix string) (bool, error)
}

type Getter func(key string) ([]byte, error)

type IteratorAction func(key string) error

func Copy(from KV, to KV) error {
	return from.IterateAll(func(key string) error {
		data, err := from.Get(key)
		if err != nil {
			return err
		}
		return to.Put(key, data)
	})
}

func Create(path string) (KV, error) {
	parts := strings.Split(path, ":")
	if len(parts) == 1 {
		return &DirKV{
			Path: parts[0],
		}, nil
	} else if parts[0] == "pebble" {
		return CreatePebble(parts[1])

	} else if parts[0] == "sql" {
		return CreateSqliteKV(parts[1])
	} else {
		return nil, errors.New("Unknown protocol " + parts[0])
	}
}
