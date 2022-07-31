package window

func lastPo2(require uint64) uint64 {
	if require == 0 {
		panic("window: measure length must be greater than or equal to 1ms")
	}
	po2 := uint64(1)
	for {
		if require < po2<<1 {
			return po2
		}
		po2 <<= 1
	}
}

func nextPo2(ensure uint64) uint64 {
	if ensure == 0 {
		return 1
	}
	po2 := uint64(1)
	for {
		if ensure <= po2 {
			return po2
		}
		po2 <<= 1
	}
}
