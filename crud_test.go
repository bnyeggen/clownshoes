package clownshoes

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestDBCRUD(t *testing.T) {
	f, e := ioutil.TempFile("", "ClownshoesDBTest")
	if e != nil {
		t.Error("Problem creating db", e)
	}
	f.Close()
	db := NewDB(f.Name())
	defer os.Remove(f.Name())

	doc1 := NewDocument([]byte("Spiffy Document 1"))
	doc2 := NewDocument([]byte("Critical Document 2"))
	doc3 := NewDocument([]byte("Important Document 3"))
	db.PutDocument(doc1)
	db.PutDocument(doc2)
	db.PutDocument(doc3)

	if len(db.GetDocuments(func(b []byte) bool {
		return true
	})) != 3 {
		t.Error("Not all documents inserted")
	}

	//Remove first document
	db.RemoveDocuments(func(payload []byte) bool {
		return bytes.Equal(payload, doc1.Payload)
	})

	docs := db.GetDocuments(func(b []byte) bool {
		return true
	})

	if len(docs) != 2 {
		t.Error("Document not successfully removed")
	}

	docs = db.GetDocuments(func(b []byte) bool {
		return bytes.Equal(b, doc2.Payload)
	})
	if len(docs) != 1 {
		t.Error("Document not found")
	}
	docs = db.GetDocuments(func(b []byte) bool {
		return bytes.Equal(b, doc3.Payload)
	})
	if len(docs) != 1 {
		t.Error("Document not found")
	}
	db.RemoveDocuments(func(payload []byte) bool {
		return true
	})

	docs = db.GetDocuments(func(b []byte) bool {
		return true
	})
	if len(docs) != 0 {
		t.Error("Surplus documents")
	}

	if db.getFirstDocOffset() != 0 || db.getLastDocOffset() != 0 {
		t.Error("db in invalid state")
	}
}
