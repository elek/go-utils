package kv

import (
	"github.com/stretchr/testify/assert"
	"os"
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
