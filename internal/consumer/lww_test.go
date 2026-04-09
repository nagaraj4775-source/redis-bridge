package consumer

import (
	"testing"
)

// TestLWW tests the Last-Write-Wins conflict resolution logic.
func TestLWWHigherHLCWins(t *testing.T) {
	localHLC := uint64(1000)
	localSite := "cluster-a"

	deltaHLC := uint64(2000)
	deltaSite := "cluster-b"

	if !shouldAccept(localHLC, localSite, deltaHLC, deltaSite) {
		t.Fatal("higher HLC should always win")
	}
}

func TestLWWLowerHLCLoses(t *testing.T) {
	localHLC := uint64(2000)
	localSite := "cluster-a"

	deltaHLC := uint64(1000)
	deltaSite := "cluster-b"

	if shouldAccept(localHLC, localSite, deltaHLC, deltaSite) {
		t.Fatal("lower HLC should lose")
	}
}

func TestLWWEqualHLCHigherSiteWins(t *testing.T) {
	localHLC := uint64(1000)
	localSite := "cluster-a"

	deltaHLC := uint64(1000)
	deltaSite := "cluster-b" // "cluster-b" > "cluster-a"

	if !shouldAccept(localHLC, localSite, deltaHLC, deltaSite) {
		t.Fatal("equal HLC: higher site_id should win")
	}
}

func TestLWWEqualHLCLowerSiteLoses(t *testing.T) {
	localHLC := uint64(1000)
	localSite := "cluster-b"

	deltaHLC := uint64(1000)
	deltaSite := "cluster-a" // "cluster-a" < "cluster-b"

	if shouldAccept(localHLC, localSite, deltaHLC, deltaSite) {
		t.Fatal("equal HLC: lower site_id should lose")
	}
}

func TestLWWFirstWriteAlwaysApplied(t *testing.T) {
	// localHLC=0 means no meta exists — first write always accepted
	localHLC := uint64(0)

	deltaHLC := uint64(500)
	deltaSite := "cluster-c"

	if !shouldAcceptFirstWrite(localHLC, deltaHLC, deltaSite) {
		t.Fatal("first write (no meta) should always be accepted")
	}
}

// shouldAccept implements the LWW resolution logic used by the consumer.
func shouldAccept(localHLC uint64, localSite string, deltaHLC uint64, deltaSite string) bool {
	if deltaHLC > localHLC {
		return true
	}
	if deltaHLC == localHLC && deltaSite > localSite {
		return true
	}
	return false
}

// shouldAcceptFirstWrite handles the case where no local meta exists.
func shouldAcceptFirstWrite(localHLC uint64, deltaHLC uint64, deltaSite string) bool {
	if localHLC == 0 {
		return true // No meta = first write
	}
	return shouldAccept(localHLC, "", deltaHLC, deltaSite)
}
