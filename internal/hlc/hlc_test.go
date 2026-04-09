package hlc

import (
	"sync"
	"testing"
)

func TestNowMonotonicity(t *testing.T) {
	h := New()
	prev := h.Now()
	for i := 0; i < 10000; i++ {
		curr := h.Now()
		if curr <= prev {
			t.Fatalf("monotonicity violated: prev=%d curr=%d at iteration %d", prev, curr, i)
		}
		prev = curr
	}
}

func TestNowConcurrentMonotonicity(t *testing.T) {
	h := New()
	const goroutines = 100
	const iterations = 1000

	results := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		results[g] = make([]uint64, iterations)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				results[g][i] = h.Now()
			}
		}()
	}
	wg.Wait()

	// Each goroutine's sequence must be strictly increasing
	for g := 0; g < goroutines; g++ {
		for i := 1; i < iterations; i++ {
			if results[g][i] <= results[g][i-1] {
				t.Fatalf("goroutine %d: monotonicity violated at index %d: %d <= %d",
					g, i, results[g][i], results[g][i-1])
			}
		}
	}

	// All timestamps globally must be unique
	seen := make(map[uint64]bool)
	for g := 0; g < goroutines; g++ {
		for i := 0; i < iterations; i++ {
			ts := results[g][i]
			if seen[ts] {
				t.Fatalf("duplicate timestamp %d from goroutine %d index %d", ts, g, i)
			}
			seen[ts] = true
		}
	}
}

func TestUpdateAdvancesPastReceived(t *testing.T) {
	h := New()
	local := h.Now()

	// Simulate a received timestamp far in the future
	futureWall := int64(9999999999999)
	futureLogical := uint32(42)
	received := (uint64(futureWall) << logicalBits) | uint64(futureLogical)

	updated := h.Update(received)
	if updated <= received {
		t.Fatalf("Update() must produce timestamp > received: updated=%d received=%d", updated, received)
	}
	_ = local
}

func TestUpdateLocalAhead(t *testing.T) {
	h := New()
	// Advance local clock several times
	for i := 0; i < 100; i++ {
		h.Now()
	}
	localBefore := h.Now()

	// Send a very old received timestamp
	oldReceived := uint64(1000 << logicalBits)
	updated := h.Update(oldReceived)

	if updated <= localBefore {
		t.Fatalf("Update() with old received must still advance: updated=%d localBefore=%d",
			updated, localBefore)
	}
}

func TestUnpack(t *testing.T) {
	h := New()
	ts := h.Now()

	wallMs, logical := Unpack(ts)
	if wallMs <= 0 {
		t.Fatalf("unpacked wallMs should be positive: %d", wallMs)
	}
	if logical > uint32(logicalMask) {
		t.Fatalf("logical counter exceeds mask: %d", logical)
	}

	// Re-pack and verify roundtrip
	repacked := (uint64(wallMs) << logicalBits) | uint64(logical)
	if repacked != ts {
		t.Fatalf("unpack/repack roundtrip failed: original=%d repacked=%d", ts, repacked)
	}
}

func TestTieBreakDeterminism(t *testing.T) {
	// Two HLCs at the exact same wall time with same logical counter
	// produce the same packed value — tie-break is by site_id (external)
	wallMs := int64(1700000000000)
	logical := uint32(5)
	packed := (uint64(wallMs) << logicalBits) | uint64(logical)

	w, l := Unpack(packed)
	if w != wallMs || l != logical {
		t.Fatalf("expected wall=%d logical=%d, got wall=%d logical=%d", wallMs, logical, w, l)
	}
}
