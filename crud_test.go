package clownshoes

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestDBCreateReadDelete(t *testing.T) {
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

	db.Sync()
	if len(db.GetDocuments(func(b []byte) bool {
		return true
	})) != 3 {
		t.Error("Documents not retrievable after sync")
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

func TestDBUpdate(t *testing.T) {
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

	//Replace with shorter
	db.ReplaceDocuments(func(payload []byte) ([]byte, bool) {
		return []byte(strings.Replace(string(payload), "Document", "Data", -1)), true
	})

	docs := db.GetDocuments(func(b []byte) bool {
		return strings.Contains(string(b), "Data")
	})
	if len(docs) != 3 {
		t.Error("Documents not found")
	}

	//Replace with longer, but inside gap
	db.ReplaceDocuments(func(payload []byte) ([]byte, bool) {
		return []byte(strings.Replace(string(payload), "Data", "Stuff", -1)), true
	})

	docs = db.GetDocuments(func(b []byte) bool {
		return strings.Contains(string(b), "Stuff")
	})
	if len(docs) != 3 {
		t.Error("Documents not found")
	}

	//Replace with longer than the "original"
	db.ReplaceDocuments(func(payload []byte) ([]byte, bool) {
		return []byte(strings.Replace(string(payload), "Stuff", "Information", -1)), true
	})

	docs = db.GetDocuments(func(b []byte) bool {
		return strings.Contains(string(b), "Information")
	})
	if len(docs) != 3 {
		t.Error("Documents not found")
	}
}

func TestDBCompaction(t *testing.T) {
	f, e := ioutil.TempFile("", "ClownshoesDBTest")
	if e != nil {
		t.Error("Problem creating db", e)
	}
	f.Close()
	db := NewDB(f.Name())
	db.Compact()

	defer os.Remove(f.Name())

	doc1 := NewDocument([]byte("50 byte document lorem ipsum z"))
	doc2 := NewDocument([]byte("Another 50 byte document herez"))

	t.Log("Adding documents")
	//1.5gb
	for i := 0; i < 15000000; i++ {
		db.PutDocument(doc1)
		db.PutDocument(doc2)
	}

	t.Log("Removing documents")
	db.RemoveDocuments(func(b []byte) bool {
		return bytes.Equal(b, doc1.Payload)
	})

	if len(db.GetDocuments(func(b []byte) bool {
		return true
	})) != 15000000 {
		t.Error("Missing documents")
	}

	t.Log("Compacting")
	db.Compact()
}
