package kv

import (
	"encoding/json"
)

type JsonCodec struct {
}

func (j JsonCodec) Decode(bytes []byte) (interface{}, error) {
	var result interface{}
	err := json.Unmarshal(bytes, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func (j JsonCodec) Encode(i interface{}) ([]byte, error) {
	jsonContent, err := json.Marshal(i)
	if err != nil {
		return []byte{}, err
	}
	return jsonContent, nil
}

func NewJsonKV(kv KV) EncodedKV {
	return EncodedKV{
		delegate: kv,
		codec:    JsonCodec{},
	}
}
