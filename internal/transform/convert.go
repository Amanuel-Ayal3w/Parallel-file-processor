package transform

import (
	"fmt"
	"strconv"
	"strings"
)

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func asBool(v any) (bool, bool) {
	switch n := v.(type) {
	case bool:
		return n, true
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(n))
		return b, err == nil
	case int:
		return n != 0, true
	case int64:
		return n != 0, true
	case float64:
		return n != 0, true
	default:
		return false, false
	}
}

func same(a, b any) bool {
	af, aok := asFloat(a)
	bf, bok := asFloat(b)
	if aok && bok {
		return af == bf
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}
