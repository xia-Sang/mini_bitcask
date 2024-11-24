package sql

// 实现数据对比 比较操作
// 目的是 实现where语句中的条件匹配
import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/constraints"
)

// Compare compares two values of type T using the provided operator.
// T must be a type that supports comparison (e.g., int, float64, string).
func Compare[T constraints.Ordered](a, b T, operator string) (bool, error) {
	switch operator {
	case "=":
		return a == b, nil
	case "!=":
		return a != b, nil
	case "<":
		return a < b, nil
	case "<=":
		return a <= b, nil
	case ">":
		return a > b, nil
	case ">=":
		return a >= b, nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", operator)
	}
}

// CompareValues compares two byte slices based on the field type and operator.
func CompareValues(fieldType FieldType, a, b []byte, operator string) (bool, error) {
	switch fieldType {
	case FieldTypeString:
		return Compare(string(a), string(b), operator)
	case FieldTypeInt:
		av, err := strconv.Atoi(string(a))
		if err != nil {
			return false, fmt.Errorf("invalid integer value: %s", a)
		}
		bv, err := strconv.Atoi(string(b))
		if err != nil {
			return false, fmt.Errorf("invalid integer value: %s", b)
		}
		return Compare(av, bv, operator)
	case FieldTypeFloat:
		av, err := strconv.ParseFloat(string(a), 64)
		if err != nil {
			return false, fmt.Errorf("invalid float value: %s", a)
		}
		bv, err := strconv.ParseFloat(string(b), 64)
		if err != nil {
			return false, fmt.Errorf("invalid float value: %s", b)
		}
		return Compare(av, bv, operator)
	case FieldTypeBool:
		av, err := parseBoolSafe(a)
		if err != nil {
			return false, err
		}
		bv, err := parseBoolSafe(b)
		if err != nil {
			return false, err
		}
		return compareBools(av, bv, operator)
	case FieldTypeDate, FieldTypeTime, FieldTypeTimestamp:
		layout := getDateLayout(fieldType)
		av, err := parseDateSafe(a, layout)
		if err != nil {
			return false, err
		}
		bv, err := parseDateSafe(b, layout)
		if err != nil {
			return false, err
		}
		return Compare(av, bv, operator)
	case FieldTypeBytes:
		return Compare(bytes.Compare(a, b), 0, operator)
	default:
		return false, fmt.Errorf("unsupported field type: %d", fieldType)
	}
}

// Helper for parsing booleans with error handling.
func parseBoolSafe(value []byte) (bool, error) {
	switch strings.ToLower(string(value)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", value)
	}
}

// Helper for parsing dates with error handling.
func parseDateSafe(value []byte, layout string) (int64, error) {
	t, err := time.Parse(layout, string(value))
	if err != nil {
		return 0, fmt.Errorf("invalid date/time value: %s", value)
	}
	return t.Unix(), nil
}

// Helper for determining date/time format based on the field type.
func getDateLayout(fieldType FieldType) string {
	switch fieldType {
	case FieldTypeDate:
		return "2006-01-02"
	case FieldTypeTime:
		return "15:04:05"
	case FieldTypeTimestamp:
		return "2006-01-02 15:04:05"
	default:
		return ""
	}
}

// Helper for comparing boolean values with supported operators.
func compareBools(a, b bool, operator string) (bool, error) {
	switch operator {
	case "=":
		return a == b, nil
	case "!=":
		return a != b, nil
	default:
		return false, fmt.Errorf("unsupported operator for boolean: %s", operator)
	}
}
