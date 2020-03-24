package prompb

func (m Sample) T() int64   { return m.Timestamp }
func (m Sample) V() float64 { return m.Value }
