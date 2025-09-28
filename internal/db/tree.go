package db

const (
	bucketSize    = 1 << 4
	bucketMinSize = bucketSize / 2
)

type Tree struct {
	root *Page
}

type key []byte
type entry []byte

func (k key) less(other key) bool {
	if len(k) != len(other) {
		return len(k) < len(other)
	}

	for i := 0; i < len(k); i++ {
		if k[i] != other[i] {
			return k[i] < other[i]
		}
	}

	return false
}
