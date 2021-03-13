package kv

import (
	"github.com/stretchr/testify/assert"
	"os"
	"sort"
	"strconv"
	"testing"
)

func TestCreateSqliteKV(t *testing.T) {
	kv, err := CreateSqliteKV("/tmp/test?batch=100")
	defer kv.Close()
	assert.Nil(t, err)
	assert.Equal(t, 100, kv.transactionSize)
}

func TestCreateSqliteKVNormal(t *testing.T) {
	kv, err := CreateSqliteKV("/tmp/test")
	defer kv.Close()
	assert.Nil(t, err)
	assert.Equal(t, 0, kv.transactionSize)
}


func TestTxHandling(t *testing.T) {
	os.Remove("/tmp/test")
	kv, err := CreateSqliteKV("/tmp/test?batch=4")
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		err = kv.Put("key"+strconv.Itoa(i), []byte("asd"))
		assert.Nil(t, err)
	}
	err = kv.Commit()
	assert.Nil(t, err)

	keys, err := kv.List("")
	assert.Nil(t, err)
	assert.Equal(t, 10, len(keys))
}

func TestPrefixTable(t *testing.T) {
	os.Remove("/tmp/test")
	kv, err := CreateSqliteKV("/tmp/test")

	err = kv.Put("key1/a1/key3", []byte("asd"))
	assert.Nil(t, err)

	err = kv.Put("key1/a1/key4", []byte("asd"))
	assert.Nil(t, err)

	err = kv.Put("key1/a2/key4", []byte("asd"))
	assert.Nil(t, err)

	err = kv.Put("key1/a2/key5", []byte("asd"))
	assert.Nil(t, err)

	keys, err := kv.List("key1")
	sort.Strings(keys)

	expected := []string{"key1/a1", "key1/a2"}
	sort.Strings(expected)

	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, expected, keys)


	keys, err = kv.List("key1/a1")
	sort.Strings(keys)

	expected = []string{"key1/a1/key3", "key1/a1/key4"}
	sort.Strings(expected)

	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, expected, keys)


}
