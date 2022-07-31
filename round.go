package window

import "time"

func windowNowRound(w *po2Window) (round int64, nowOffset uint64) {
	nowMilli := NowFunc().Truncate(time.Millisecond).UnixMilli()
	offset := (nowMilli - w.initUnixMilli) >> w.bucketMilliBits
	return nowMilli >> w.roundMilliBits, uint64(offset) & w.bucketMask
}
