package tenderduty

// gnoWindow is a fixed-capacity sliding window of recent block outcomes used
// only by the gnoland provider. gno.land (TM2) has no on-chain slashing module,
// so there is no signed_blocks_window to query; this LOCAL window gives the
// dashboard uptime % and the percentage_missed alert a bounded, sliding
// denominator (cosmos-like) instead of an ever-growing cumulative counter.
//
// It is NOT a slashing window — gno does not slash. It is a signing-health
// window: of the last N polled blocks, how many did the validator miss.
type gnoWindow struct {
	buf    []bool // circular buffer; true = missed block
	head   int    // next write index (= oldest entry when full)
	size   int    // valid entries (<= cap)
	cap    int    // max entries (= gno_signed_blocks_window)
	missed int    // count of true entries currently in buf
}

// newGnoWindow returns a sliding window of the given capacity. cap < 1 clamps to 1.
func newGnoWindow(cap int) *gnoWindow {
	if cap < 1 {
		cap = 1
	}
	return &gnoWindow{buf: make([]bool, cap), cap: cap}
}

// Push records one block outcome (missed=true). When full, the oldest entry is
// evicted. Missed()/Window() are maintained incrementally (O(1)).
func (w *gnoWindow) Push(missed bool) {
	if w.size == w.cap {
		if w.buf[w.head] { // evict oldest (head points at it when full)
			w.missed--
		}
	} else {
		w.size++
	}
	w.buf[w.head] = missed
	if missed {
		w.missed++
	}
	w.head = (w.head + 1) % w.cap
}

// Missed returns the count of missed blocks currently in the window.
func (w *gnoWindow) Missed() int { return w.missed }

// Window returns the number of blocks currently in the window (grows 0->cap
// during warm-up, then stable at cap).
func (w *gnoWindow) Window() int { return w.size }
