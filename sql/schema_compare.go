package sql

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

// Helper functions for generic comparisons
func less[T constraints.Ordered](a, b T) bool           { return a < b }
func lessOrEqual[T constraints.Ordered](a, b T) bool    { return a <= b }
func greater[T constraints.Ordered](a, b T) bool        { return a > b }
func greaterOrEqual[T constraints.Ordered](a, b T) bool { return a >= b }

// CompareValues compares two byte slices based on the field type and operator.
func CompareValues(fieldType FieldType, a, b []byte, operator string) (bool, error) {
	switch fieldType {
	case FieldTypeString:
		return Compare[string](string(a), string(b), operator)
	case FieldTypeInt:
		return Compare[int](parseInt(a), parseInt(b), operator)
	case FieldTypeFloat:
		return Compare[float64](parseFloat(a), parseFloat(b), operator)
	case FieldTypeBool:
		return compareBools(parseBool(a), parseBool(b), operator)
	case FieldTypeDate:
		return Compare[int64](parseDate(a, "2006-01-02"), parseDate(b, "2006-01-02"), operator)
	case FieldTypeTime:
		return Compare[int64](parseDate(a, "15:04:05"), parseDate(b, "15:04:05"), operator)
	case FieldTypeTimestamp:
		return Compare[int64](parseDate(a, "2006-01-02 15:04:05"), parseDate(b, "2006-01-02 15:04:05"), operator)
	case FieldTypeBytes:
		return Compare[int](bytes.Compare(a, b), 0, operator)
	default:
		return false, fmt.Errorf("unsupported field type: %d", fieldType)
	}
}

// Helper functions for parsing values
func parseInt(value []byte) int {
	num, err := strconv.Atoi(string(value))
	if err != nil {
		panic(fmt.Sprintf("invalid integer value: %s", value))
	}
	return num
}

func parseFloat(value []byte) float64 {
	num, err := strconv.ParseFloat(string(value), 64)
	if err != nil {
		panic(fmt.Sprintf("invalid float value: %s", value))
	}
	return num
}

func parseBool(value []byte) bool {
	switch strings.ToLower(string(value)) {
	case "true":
		return true
	case "false":
		return false
	default:
		panic(fmt.Sprintf("invalid boolean value: %s", value))
	}
}

// compareBools compares two boolean values with supported operators (=, !=).
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

func parseDate(value []byte, layout string) int64 {
	t, err := time.Parse(layout, string(value))
	if err != nil {
		panic(fmt.Sprintf("invalid date/time value: %s", value))
	}
	return t.Unix()
}
