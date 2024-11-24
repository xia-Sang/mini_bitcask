package sql

// 支持where表达式
// 目前仅仅只能支持多条where的and操作
// 复杂操作并不支持
type Condition struct {
	Operator string // e.g., "=", "!=", "<", "<=", ">", ">="
	Value    []byte // Value to compare against
}

//
