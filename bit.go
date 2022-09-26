package window

func countContinuouslyBits(i uint64) int {
	bits, count := 0, i
	for count&1 != 0 {
		bits++
		count >>= 1
	}
	return bits
}
