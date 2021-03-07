package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJsonEncode(t *testing.T) {
	codec := JsonCodec{}
	res, err := codec.Decode([]byte("{\"asd\":\"qwe\"}"))
	assert.Nil(t, err)
	resMap := res.(map[string]interface{})
	assert.Equal(t, "qwe", resMap["asd"])
}
