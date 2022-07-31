package window

func shouldRound(nowRound, nowOffset, bucketOffset int64) int64 {
	// For example, bucketOffset is 0, nowOffset is 1 and nowRound is 3,
	// then bucket 0 and 1 should round 3, and later bucket 2, 3, 4, ..., should round 2.
	if bucketOffset <= nowOffset {
		return nowRound
	}
	return nowRound - 1
}
