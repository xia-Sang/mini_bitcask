package utils

import (
	"encoding/json"
)

// serializeRow 将 map[string][]byte 转为 JSON 字符串的 []byte 表示
func SerializeRow(rowData map[string][]byte) ([]byte, error) {
	// 将 map[string][]byte 转为 map[string]string，方便 JSON 序列化
	stringMap := make(map[string]string)
	for key, value := range rowData {
		stringMap[key] = string(value)
	}

	// 使用 JSON 序列化
	return json.Marshal(stringMap)
}

// deserializeRow 将 JSON 的 []byte 解码为 map[string][]byte
func DeserializeRow(data []byte) (map[string][]byte, error) {
	// 先将 JSON 转为 map[string]string
	stringMap := make(map[string]string)
	if err := json.Unmarshal(data, &stringMap); err != nil {
		return nil, err
	}

	// 将 map[string]string 转为 map[string][]byte
	rowData := make(map[string][]byte)
	for key, value := range stringMap {
		rowData[key] = []byte(value)
	}

	return rowData, nil
}
