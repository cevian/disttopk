package disttopk

type AlgoStats struct {
	Bytes_transferred uint64
	Serial_items  int
	Random_access int
	Random_items  int
	Length        int
	Recall        float64
	Abs_err       float64
	Rel_err       float64
	Edit_distance  float64
}

func (t *AlgoStats) Merge(other AlgoStats) {
	t.Serial_items += other.Serial_items
	t.Random_access += other.Random_access
	t.Random_items += other.Random_items
	t.Length += other.Length
}
