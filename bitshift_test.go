package clownshoes

import (
	"math/rand"
	"testing"
)

func TestBitShift(t *testing.T) {
	buffer4 := make([]byte, 4, 4)
	buffer8 := make([]byte, 8, 8)

	testval32 := rand.Uint32()
	uint32ToBytes(buffer4, 0, testval32)
	if testval32 != uint32FromBytes(buffer4, 0) {
		t.Error("uint32To/FromBytes fails on", testval32)
	}

	testval64 := uint64(rand.Int63())
	uint64ToBytes(buffer8, 0, testval64)
	if testval64 != uint64FromBytes(buffer8, 0) {
		t.Error("uint64To/FromBytes fails on", testval64)
	}
}
