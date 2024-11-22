package sql

// 支持的字段类型
const (
	FieldTypeString = "string"
	FieldTypeInt    = "int"
	FieldTypeBytes  = "bytes"
)

// 字段定义
type Field struct {
	Name string
	Type string // 字段类型，例如 "string", "int", "bytes"
}

// 表结构定义
type TableSchema struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Indexes []string `json:"indexes"`
	//todo: 需要添加约束函数
}
