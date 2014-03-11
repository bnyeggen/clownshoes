package clownshoes

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestDBCopy(t *testing.T) {
	f, _ := ioutil.TempFile("", "ClownshoesDBTest")
	f2, _ := ioutil.TempFile("", "ClownshoesDBCopy")
	f.Close()
	f2.Close()

	db := NewDB(f.Name())
	defer os.Remove(f.Name())

	doc1 := NewDocument([]byte("Spiffy Document 1"))
	doc2 := NewDocument([]byte("Critical Document 2"))
	doc3 := NewDocument([]byte("Important Document 3"))
	db.PutDocument(doc1)
	db.PutDocument(doc2)
	db.PutDocument(doc3)

	db.CopyDB(f2.Name(), "")
	db2 := NewDB(f2.Name())

	if len(db2.GetDocuments(func(b []byte) bool {
		return true
	})) != 3 {
		t.Error("Not all documents present in copy")
	}

}
