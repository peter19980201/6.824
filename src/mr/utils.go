package mr

import "math"

func max(a, b int) int {
	return int(math.Max(float64(a), float64(b)))
}
