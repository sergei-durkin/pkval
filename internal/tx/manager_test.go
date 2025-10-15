package tx

import (
	"testing"
)

func TestTxManager(t *testing.T) {
	m := NewManager()

	key := [12]byte{}
	copy(key[:], []byte("test"))

	trx := m.begin()
	ok, err := trx.Read(key)
	if !ok {
		t.Fatal("ok should be true")
	}
	if err != nil {
		t.Fatalf("unexpected err %s", err.Error())
	}

	trx2 := m.begin()
	ok, err = trx2.Read(key)
	if !ok {
		t.Fatal("ok should be true")
	}
	if err != nil {
		t.Fatalf("unexpected err %s", err.Error())
	}

	ok = trx2.Commit()
	if !ok {
		t.Fatal("ok should be true")
	}

	ok, err = trx.Upgrade(key)
	if !ok {
		t.Fatal("ok should be true")
	}
	if err != nil {
		t.Fatalf("unexpected err %s", err.Error())
	}

	ok = trx.Commit()
	if !ok {
		t.Fatal("ok should be true")
	}
}

func TestTxManagerDeadlock(t *testing.T) {
	m := NewManager()

	key := [12]byte{}
	copy(key[:], []byte("test"))
	key2 := [12]byte{}
	copy(key[:], []byte("test_2"))

	trx := m.begin()
	trx2 := m.begin()

	ok, err := trx.Read(key)
	if !ok {
		t.Fatal("ok should be true")
	}
	if err != nil {
		t.Fatalf("unexpected err %s", err.Error())
	}

	ok, err = trx2.Read(key2)
	if !ok {
		t.Fatal("ok should be true")
	}
	if err != nil {
		t.Fatalf("unexpected err %s", err.Error())
	}

	deadlockCh := make(chan error)
	go func() {
		_, err := trx2.Write(key)
		trx2.Abort()

		deadlockCh <- err
	}()

	ok, err = trx.Write(key2)
	if err != nil {
		t.Fatalf("unexpected err %s", err.Error())
	}
	if ok {
		t.Fatal("ok should be false")
	}

	ok = trx.Commit()
	if !ok {
		t.Fatal("ok should be true")
	}

	err = <-deadlockCh
	if err != errDeadlock {
		t.Fatalf("unexpected err %s", err.Error())
	}
}
