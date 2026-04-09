package applier

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nagaraju/redibridge/internal/bus"
	"github.com/nagaraju/redibridge/internal/reader"
)

func TestDeltaStringEncoding(t *testing.T) {
	d := bus.Delta{
		SiteID:     "cluster-a",
		Key:        "mykey",
		KeyType:    "string",
		Value:      []byte("hello"),
		HLC:        123456,
		CapturedAt: time.Now().UnixMilli(),
		TTLMs:      -1,
		Cmd:        "SET",
		SeqID:      "cluster-a-1",
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var d2 bus.Delta
	if err := json.Unmarshal(b, &d2); err != nil {
		t.Fatal(err)
	}
	if string(d2.Value) != "hello" {
		t.Fatalf("expected 'hello', got '%s'", string(d2.Value))
	}
}

func TestDeltaHashEncoding(t *testing.T) {
	fields := map[string]string{"f1": "v1", "f2": "v2"}
	b, _ := json.Marshal(fields)
	d := bus.Delta{KeyType: "hash", Value: b}

	var decoded map[string]string
	if err := json.Unmarshal(d.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["f1"] != "v1" || decoded["f2"] != "v2" {
		t.Fatalf("hash decode mismatch: %v", decoded)
	}
}

func TestDeltaListEncoding(t *testing.T) {
	elements := []string{"a", "b", "c"}
	b, _ := json.Marshal(elements)
	d := bus.Delta{KeyType: "list", Value: b}

	var decoded []string
	if err := json.Unmarshal(d.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 3 || decoded[0] != "a" {
		t.Fatalf("list decode mismatch: %v", decoded)
	}
}

func TestDeltaSetEncoding(t *testing.T) {
	members := []string{"x", "y", "z"}
	b, _ := json.Marshal(members)
	d := bus.Delta{KeyType: "set", Value: b}

	var decoded []string
	if err := json.Unmarshal(d.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 3 {
		t.Fatalf("set decode mismatch: %v", decoded)
	}
}

func TestDeltaZSetEncoding(t *testing.T) {
	entries := []reader.ZSetEntry{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.5},
	}
	b, _ := json.Marshal(entries)
	d := bus.Delta{KeyType: "zset", Value: b}

	var decoded []reader.ZSetEntry
	if err := json.Unmarshal(d.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 || decoded[0].Member != "a" || decoded[1].Score != 2.5 {
		t.Fatalf("zset decode mismatch: %v", decoded)
	}
}

func TestTTLExpiredInTransit(t *testing.T) {
	// Simulate a delta captured 10 seconds ago with 5s TTL
	d := bus.Delta{
		Key:        "expired-key",
		KeyType:    "string",
		Value:      []byte("val"),
		CapturedAt: time.Now().UnixMilli() - 10000,
		TTLMs:      5000,
	}
	elapsed := time.Now().UnixMilli() - d.CapturedAt
	remaining := d.TTLMs - elapsed
	if remaining > 0 {
		t.Fatal("key should be expired in transit")
	}
}

func TestTTLPreserved(t *testing.T) {
	now := time.Now().UnixMilli()
	d := bus.Delta{
		Key:        "ttl-key",
		KeyType:    "string",
		Value:      []byte("val"),
		CapturedAt: now - 100, // 100ms ago
		TTLMs:      5000,      // 5 second TTL
	}
	remaining := d.TTLMs - (time.Now().UnixMilli() - d.CapturedAt)
	if remaining < 4800 || remaining > 5000 {
		t.Fatalf("TTL should be ~4900ms, got %d", remaining)
	}
}
