package libs

import (
	"math"
)

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func ToFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func CheckStrEmpty(args ...string) (int, bool) {
	for k, val := range args {
		if 0 == len(val) {
			return k, false
		}
	}
	return -1, true
}

func CheckIntZero(args ...int) (int, bool) {
	for k, val := range args {
		if 0 > val {
			return k, false
		}
	}
	return -1, true
}
