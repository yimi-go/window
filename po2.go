package window

func lastPo2(require int64) int64 {
	if require == 0 {
		panic("window: measure length must be greater than or equal to 1ms")
	}
	po2 := int64(1)
	for {
		if require < po2<<1 {
			return po2
		}
		po2 <<= 1
	}
}

func nextGreaterPo2(ensure int64) int64 {
	if ensure == 0 {
		return 1
	}
	po2 := int64(1)
	for {
		if ensure < po2 {
			return po2
		}
		po2 <<= 1
	}
}
