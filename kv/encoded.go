package kv

import (
	"io"
	"time"
)

type TypedKV interface {
	Put(key string, value []interface{}) error
	List(prefix string) ([]string, error)
	IterateAll(action IteratorAction) error
	Iterate(prefix string, action IteratorAction) error
	IterateSubTree(prefix string, action IteratorAction) error
	Contains(key string) bool
	GetOrDefault(key string, defaultFunc DefaultProvider) (interface{}, error)
	Get(prefix string) (interface{}, error)
	GetReader(prefix string) (io.Reader, error)
	IsChanged(since time.Time, prefix string) (bool, error)
}

type EncodedKV struct {
	delegate KV
	codec    Codec
}

type DefaultProvider func(key string) (interface{}, error)

func (e EncodedKV) Put(key string, value []interface{}) error {
	panic("implement me")
}

func (e EncodedKV) List(prefix string) ([]string, error) {
	return e.delegate.List(prefix)
}

func (e EncodedKV) IterateAll(action IteratorAction) error {
	return e.delegate.IterateAll(action)
}

func (e EncodedKV) Iterate(prefix string, action IteratorAction) error {
	return e.delegate.Iterate(prefix, action)
}

func (e EncodedKV) IterateSubTree(prefix string, action IteratorAction) error {
	return e.delegate.IterateSubTree(prefix, action)
}

func (e EncodedKV) Contains(key string) bool {
	return e.delegate.Contains(key)
}

func (e EncodedKV) GetOrDefault(key string, defaultFunc DefaultProvider) (interface{}, error) {
	if !e.delegate.Contains(key) {
		return defaultFunc(key)
	}
	return e.Get(key)
}

func (e EncodedKV) Get(prefix string) (interface{}, error) {
	raw, err := e.delegate.Get(prefix)
	if err != nil {
		return nil, err
	}
	value, err := e.codec.Decode(raw)
	if raw != nil {
		return nil, err
	}
	return value, nil
}

func (e EncodedKV) GetReader(prefix string) (interface{}, error) {
	return e.delegate.GetReader(prefix)
}

func (e EncodedKV) IsChanged(since time.Time, prefix string) (bool, error) {
	return e.delegate.IsChanged(since, prefix)
}
