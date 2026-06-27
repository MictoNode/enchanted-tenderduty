package tenderduty

import "testing"

func TestGnoWindow_UnderCapacity(t *testing.T) {
	w := newGnoWindow(5)
	w.Push(true)
	w.Push(true)
	w.Push(false)
	if got := w.Window(); got != 3 {
		t.Fatalf("Window = %d, want 3", got)
	}
	if got := w.Missed(); got != 2 {
		t.Fatalf("Missed = %d, want 2", got)
	}
}

func TestGnoWindow_AtCapacity(t *testing.T) {
	w := newGnoWindow(3)
	for i := 0; i < 3; i++ {
		w.Push(true)
	}
	if got := w.Window(); got != 3 {
		t.Fatalf("Window = %d, want 3", got)
	}
	if got := w.Missed(); got != 3 {
		t.Fatalf("Missed = %d, want 3", got)
	}
}

func TestGnoWindow_EvictionDecrementsMissed(t *testing.T) {
	// cap 3: push miss,miss,miss,sign,sign -> 2 misses age out, 1 remains
	w := newGnoWindow(3)
	w.Push(true)
	w.Push(true)
	w.Push(true)
	w.Push(false)
	w.Push(false)
	if got := w.Window(); got != 3 {
		t.Fatalf("Window = %d, want 3", got)
	}
	if got := w.Missed(); got != 1 {
		t.Fatalf("Missed = %d, want 1 (2 aged out)", got)
	}
}

func TestGnoWindow_MixedEviction(t *testing.T) {
	// cap 4: [miss,sign,miss,sign] missed=2; push miss -> evict miss(-1)+add miss(+1)=2
	w := newGnoWindow(4)
	w.Push(true)
	w.Push(false)
	w.Push(true)
	w.Push(false)
	w.Push(true)
	if got := w.Missed(); got != 2 {
		t.Fatalf("Missed = %d, want 2", got)
	}
	if got := w.Window(); got != 4 {
		t.Fatalf("Window = %d, want 4", got)
	}
}

func TestGnoWindow_InvalidCapClampsToOne(t *testing.T) {
	w := newGnoWindow(0)
	w.Push(true)
	if got := w.Window(); got != 1 {
		t.Fatalf("Window = %d, want 1 (clamped cap)", got)
	}
}
