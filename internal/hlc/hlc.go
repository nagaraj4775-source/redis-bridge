package hlc

import (
	"sync"
	"time"
)

const (
	// Upper 44 bits for wall clock ms, lower 20 bits for logical counter
	logicalBits = 20
	logicalMask = (1 << logicalBits) - 1
	wallMask    = ^uint64(logicalMask)
)

// HLC implements a Hybrid Logical Clock.
// Thread-safe — all methods hold the mutex.
type HLC struct {
	mu      sync.Mutex
	wallMs  int64
	logical uint32
}

// New creates a new HLC initialized to the current wall clock.
func New() *HLC {
	return &HLC{
		wallMs:  time.Now().UnixMilli(),
		logical: 0,
	}
}

// Now returns a monotonically increasing uint64 timestamp.
// Upper 44 bits = wall clock ms, lower 20 bits = logical counter.
func (h *HLC) Now() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().UnixMilli()

	if now > h.wallMs {
		h.wallMs = now
		h.logical = 0
	} else {
		h.logical++
	}

	return h.pack(h.wallMs, h.logical)
}

// Update advances the clock to max(local, received) + 1.
// Returns the new timestamp.
func (h *HLC) Update(received uint64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	recvWall := int64((received & wallMask) >> logicalBits)
	recvLogical := uint32(received & uint64(logicalMask))

	now := time.Now().UnixMilli()

	if now > h.wallMs && now > recvWall {
		h.wallMs = now
		h.logical = 0
	} else if recvWall > h.wallMs {
		h.wallMs = recvWall
		h.logical = recvLogical + 1
	} else if h.wallMs > recvWall {
		h.logical++
	} else {
		// h.wallMs == recvWall
		if recvLogical > h.logical {
			h.logical = recvLogical + 1
		} else {
			h.logical++
		}
	}

	return h.pack(h.wallMs, h.logical)
}

// Pack combines wall clock ms and logical counter into a single uint64.
func (h *HLC) pack(wallMs int64, logical uint32) uint64 {
	return (uint64(wallMs) << logicalBits) | uint64(logical&uint32(logicalMask))
}

// Unpack extracts wall clock ms and logical counter from a packed uint64.
func Unpack(ts uint64) (wallMs int64, logical uint32) {
	wallMs = int64((ts & wallMask) >> logicalBits)
	logical = uint32(ts & uint64(logicalMask))
	return
}
