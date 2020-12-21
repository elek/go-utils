package kv

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func getKvs() []KV {
	kvs := make([]KV, 0)
	kvs = append(kvs, &DirKV{
		Path: "/tmp/testx",
	})
	pebble, err := Open("/tmp/pebble")
	if err != nil {
		panic(err)
	}
	kvs = append(kvs, &pebble)
	return kvs
}
func TestPutGet(t *testing.T) {
	for _, kv := range getKvs() {
		err := kv.Put("asd", []byte("value1"))
		assert.Nil(t, err)

		get, err := kv.Get("asd")
		assert.Nil(t, err)
		assert.Equal(t, []byte("value1"), get)
	}
}

func TestList(t *testing.T) {
	for _, kv := range getKvs() {
		err := kv.Put("key1", []byte("value1"))
		assert.Nil(t, err)

		err = kv.Put("dir1/key1", []byte("value1"))
		assert.Nil(t, err)

		err = kv.Put("dir1/key2", []byte("value1"))
		assert.Nil(t, err)

		err = kv.Put("dir1/dir2/key3", []byte("value1"))
		assert.Nil(t, err)

		list, err := kv.List("dir1")
		assert.Nil(t, err)

		val, err := kv.Get("dir1/key1")
		assert.Nil(t, err)
		assert.Equal(t, []byte("value1"), val)

		expected := []string{"dir1/key1", "dir1/key2", "dir1/dir2"}
		sort.Strings(list)
		sort.Strings(expected)
		assert.Equal(t, expected, list)
	}
}

func TestIterator(t *testing.T) {
	for _, kv := range getKvs() {
		result := make([]string, 0)

		err := kv.Put("key1", []byte("value1"))
		assert.Nil(t, err)

		err = kv.Put("dir1/key1", []byte("value1"))
		assert.Nil(t, err)

		err = kv.Put("dir1/key2", []byte("value1"))
		assert.Nil(t, err)

		err = kv.Put("dir1/dir2/key3", []byte("value1"))
		assert.Nil(t, err)

		err = kv.IterateAll(func(key string) error {
			result = append(result, key)
			return nil
		})
		assert.Nil(t, err)

		expected := []string{"dir1/key1", "dir1/key2", "key1", "dir1/dir2/key3"}
		sort.Strings(result)
		sort.Strings(expected)
		assert.Equal(t, expected, result)
	}
}
