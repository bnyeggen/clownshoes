package clownshoes

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func first2Bytes(b []byte) string {
	return string(b[:2])
}

func randAscii(n int) []byte {
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = byte(rand.Int31n(94)) + 33
	}
	return out
}

func TestIndexing(t *testing.T) {
	f, e := ioutil.TempFile("", "ClownshoesDBTest")
	if e != nil {
		t.Error("Problem creating db", e)
	}
	f.Close()
	db := NewDB(f.Name())
	defer os.Remove(f.Name())
	db.AddIndex("ftb", first2Bytes)
	rawStorage := make(map[string][][]byte)

	for i := 0; i < 10000; i++ {
		b := randAscii(6)

		rawStorage[first2Bytes(b)] = append(rawStorage[first2Bytes(b)], b)
		db.PutDocument(NewDocument(b))
	}

	for k, v := range rawStorage {
		doc := db.GetDocumentsWhere("ftb", k)
		if len(doc) != len(v) {
			t.Error("Error in indexed retrieval")
		}
	}

	//Test index dump & recreation
	idxFile, e := ioutil.TempFile("", "ClownshoesDBTest")
	if e != nil {
		t.Error("Problem creating indexdump", e)
	}
	idxFileName := idxFile.Name()
	idxFile.Close()
	defer os.Remove(idxFileName)

	db.dumpIndexes(idxFileName)
	db.RemoveIndex("ftb")
	if len(db.indexes) != 0 {
		t.Error("Indexes not removed")
	}

	var idxToKeyFn = map[string]func([]byte) string{"ftb": first2Bytes}
	db.LoadIndexes(idxToKeyFn, idxFileName)
	for k, v := range rawStorage {
		doc := db.GetDocumentsWhere("ftb", k)
		if len(doc) != len(v) {
			t.Error("Error in index re-creation")
		}
	}
	var removedK string
	//Test index-based deletion
	for k, v := range rawStorage {
		removedK = k
		ct := db.RemoveDocumentsWhere("ftb", k, func([]byte) bool { return true })
		if ct != uint64(len(v)) {
			t.Error("Insufficient documents removed")
		}
		if len(db.GetDocumentsWhere("ftb", k)) != 0 {
			t.Error("Documents exist after removal")
		}
		break
	}
	delete(rawStorage, removedK)
	//And compaction after index-based deletion
	db.Compact()
	//And subsequent lookups
	for k, v := range rawStorage {
		doc := db.GetDocumentsWhere("ftb", k)
		if len(doc) != len(v) {
			t.Log(len(doc))
			t.Log(len(v))
			t.Error("Error in indexed retrieval after indexed deletion")
		}
	}
}
