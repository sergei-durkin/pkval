package tx

import (
	"testing"
)

func TestDeadlockDetector(t *testing.T) {
	d := NewDeadlockDetector()
	ok := d.Add(1, 2)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(2, 1)
	if ok {
		t.Fatal("ok should be false")
	}

	ok = d.Add(2, 1)
	if !ok {
		t.Fatal("ok should be true")
	}

	d.Remove(1)

	ok = d.Add(2, 1)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(2, 1)
	if !ok {
		t.Fatal("ok should be true")
	}
}

func TestDeadlockDetectorLongCycle(t *testing.T) {
	d := NewDeadlockDetector()

	// 1->2
	ok := d.Add(1, 2)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 2->3
	ok = d.Add(2, 3)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 3->4
	ok = d.Add(3, 4)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 4->5
	ok = d.Add(4, 5)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 5->6
	ok = d.Add(5, 6)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 6->7
	ok = d.Add(6, 7)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 7->8
	ok = d.Add(7, 8)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 8->9
	ok = d.Add(8, 9)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 9->1: cycle
	ok = d.Add(9, 1)
	if ok {
		t.Fatal("ok should be false")
	}

	// 9->1: no cycle, since Tx 9 was removed in the previous step
	ok = d.Add(9, 1)
	if !ok {
		t.Fatal("ok should be true")
	}

	// double, no cycle
	ok = d.Add(9, 1)
	if !ok {
		t.Fatal("ok should be true")
	}

	// 1->9: cycle
	ok = d.Add(1, 9)
	if ok {
		t.Fatal("ok should be false")
	}
}

func TestDeadlockDetectorComplexCycle(t *testing.T) {
	d := NewDeadlockDetector()

	ok := d.Add(1, 2)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(1, 3)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(1, 4)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(2, 3)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(2, 4)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(3, 4)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(4, 5)
	if !ok {
		t.Fatal("ok should be true")
	}

	ok = d.Add(5, 6)
	if !ok {
		t.Fatal("ok should be true")
	}

	d.Remove(2)

	ok = d.Add(2, 1)
	if !ok {
		t.Fatal("ok should be true")
	}
}
