package json

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func AsJsonList(data []byte, err error) ([]interface{}, error) {
	result := make([]interface{}, 0)
	err = json.Unmarshal(data, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func AsJson(data []byte, err error) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func Limit(str string, limit int) string {
	return str[0:min(limit, len(str))]
}

func M(data interface{}, keys ...string) interface{} {
	result := data
	for _, key := range keys {
		switch v := result.(type) {
		case map[string]interface{}:
			result = v[key]
		case map[interface{}]interface{}:
			result = v[key]
		}
		if result == nil {
			return result
		}
	}
	return result
}

func MS(data interface{}, keys ...string) string {
	value := M(data, keys...)
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%s", value)
}

//Return value as pointer to a timestamp (milliseconds)
func MTP(layout string, data interface{}, keys ...string) *int64 {
	value := MSP(data, keys...)
	if value == nil {
		return nil
	}
	t, err := time.Parse(layout, *value)
	if err != nil {
		panic(err)
	}
	res := t.Unix() * 1000
	return &res
}

func MT(layout string, data interface{}, keys ...string) int64 {
	value := MTP(layout, data, keys...)
	if value == nil {
		return 0
	}
	return *value
}

func MSP(data interface{}, keys ...string) *string {
	value := M(data, keys...)
	if value == nil {
		return nil
	}
	res := fmt.Sprintf("%s", value)
	return &res
}

func MNS(data interface{}, keys ...string) string {
	val := M(data, keys...)
	if val == nil {
		return ""
	}
	return strconv.Itoa(int(val.(float64)))
}

func MB(data interface{}, keys ...string) bool {
	val := M(data, keys...)
	if val == nil {
		return false
	} else {
		return val.(bool)
	}

}

func MBS(data interface{}, keys ...string) string {
	if MB(data, keys...) {
		return "true"
	} else {
		return "false"
	}
}

func MN(data interface{}, keys ...string) int {
	return int(M(data, keys...).(float64))
}

func MN32(data interface{}, keys ...string) int32 {
	return int32(M(data, keys...).(float64))
}

func MN64(data interface{}, keys ...string) int64 {
	return int64(M(data, keys...).(float64))
}

func L(data interface{}) []interface{} {
	if data == nil {
		return make([]interface{}, 0)
	}
	return data.([]interface{})
}

func Nilsafe(data interface{}) interface{} {
	if data == nil {
		return ""
	}
	return data
}
