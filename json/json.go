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

//Return value as pointer to a timestamp (milliseconds) epoch
func MEP(layout string, data interface{}, keys ...string) *int64 {
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

//return value as a timestamp (milliseconds) epoch
func ME(layout string, data interface{}, keys ...string) int64 {
	value := MEP(layout, data, keys...)
	if value == nil {
		return 0
	}
	return *value
}

//return value as a string pointer (optional)
func MSP(data interface{}, keys ...string) *string {
	value := M(data, keys...)
	if value == nil {
		return nil
	}
	res := fmt.Sprintf("%s", value)
	return &res
}

//return number value as a string
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
	raw := M(data, keys...)
	if raw == nil {
		return 0
	}
	return int(raw.(float64))
}

func MN32(data interface{}, keys ...string) int32 {
	return int32(M(data, keys...).(float64))
}

func MN64(data interface{}, keys ...string) int64 {
	rawValue := M(data, keys...)
	switch rawValue.(type) {
	case float64:
		return int64(M(data, keys...).(float64))
	case string:
		value, err := strconv.Atoi(rawValue.(string))
		println(rawValue.(string))
		if err != nil {
			panic("Couldn't get value as number")
		}

		return int64(value)
	default:
		panic(fmt.Sprintf("I don't know about type %T!", rawValue))
	}
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
