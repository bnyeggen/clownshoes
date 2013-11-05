package clownshoes

//This has NOT been tested yet.

import (
	"os"
	"sync"
	"syscall"
)

//Storage schema:
//A uint64 pointer to the position of the first document, or 0 if we're empty
//A uint64 pointer to the position of the last document, or 0 if we're empty
//And then a bunch o' documents

type DocumentBundle struct {
	sync.RWMutex                  //We support per-database write locks as of version 0, aren't we fancy
	AsBytes      []byte           //Entire mmap'd array.  Includes first & last doc offsets
	FileLoc      string           //Location of file we're mmaping
	indexes      map[string]index //For exact-match indexing
}

// Copies the data, overwriting if necessary, to a file at destination.  Calling
// this periodically is the only way to ensure you have a consistent version of
// your data.
func (db *DocumentBundle) CopyDB(destination string) error {
	db.Lock()
	db.Unlock()
	newFile, err := os.Create(destination)
	if err != nil {
		return err
	}
	newFile.Write(db.AsBytes)
	return newFile.Close()
}

// Return the offset of the first valid document in the DB, or 0 if there is none
func (db *DocumentBundle) GetFirstDocOffset() uint64 {
	return uint64FromBytes(db.AsBytes, 0)
}

// Return the offset of the last valid document in the DB, or 0 if it is empty
func (db *DocumentBundle) GetLastDocOffset() uint64 {
	return uint64FromBytes(db.AsBytes, 8)
}

func (db *DocumentBundle) setFirstDocOffset(offset uint64) {
	uint64ToBytes(db.AsBytes, 0, offset)
}
func (db *DocumentBundle) setLastDocOffset(offset uint64) {
	uint64ToBytes(db.AsBytes, 8, offset)
}

// For the document at the given position, update the pointer to the next document
func (db *DocumentBundle) setNextDocOffset(docOffset uint64, nextDocOffset uint64) {
	uint64ToBytes(db.AsBytes, docOffset+4, nextDocOffset)
}

// For the document at the given position, update the pointer to the previous document
func (db *DocumentBundle) setPrevDocOffset(docOffset uint64, prevDocOffset uint64) {
	uint64ToBytes(db.AsBytes, docOffset+4+8, prevDocOffset)
}

// Returns a new 1gb DocumentBundle
func NewDB(location string) *DocumentBundle {
	fileOut, _ := os.OpenFile(location, os.O_RDWR|os.O_CREATE, 0666)
	defer fileOut.Close()
	stats, _ := fileOut.Stat()
	if stats.Size() < 16 {
		//New file, clear the pointers to the start and end positions
		fileOut.Write(make([]byte, 16))
		//And give us some room
		fileOut.Truncate(1000000000)
	}
	bytesOut, _ := syscall.Mmap(int(fileOut.Fd()), 0, int(stats.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return &DocumentBundle{sync.RWMutex{}, bytesOut, location, make(map[string]index, 0)}
}

// This version does not do its own locking, so we can support callers who already
// have the lock.
func (db *DocumentBundle) doGrowDB() {
	syscall.Munmap(db.AsBytes)
	newFile, _ := os.OpenFile(db.FileLoc, os.O_RDWR|os.O_CREATE, 0666)
	defer newFile.Close()
	stats, _ := newFile.Stat()
	//Grow by 1gb
	newSize := stats.Size() + 1000000000
	newFile.Truncate(newSize)
	newArr, _ := syscall.Mmap(int(newFile.Fd()), 0, int(newSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	db.AsBytes = newArr
}

// Returns the document at the given address.  Assumes the address is valid.
func (db *DocumentBundle) GetDocumentAt(offset uint64) Document {
	db.RLock()
	defer db.RUnlock()
	return db.doGetDocumentAt(offset)
}

// Using the index with the given name, lookup the documents with the given key.
func (db *DocumentBundle) GetDocumentsWithIndex(indexName string, lookupKey interface{}) []Document {
	db.RLock()
	defer db.RUnlock()
	idx, found := db.indexes[indexName]
	if found {
		offsets := idx.lookup[lookupKey]
		out := make([]Document, 0, len(offsets))
		for _, offset := range offsets {
			out = append(out, db.doGetDocumentAt(offset))
		}
		return out
	}
	return make([]Document, 0)
}

// Run the given function over each valid offset/document pair, sequentially.
// Must not lock the DB to avoid deadlock.
// We may support different contracts wrt locking and concurrent execution or
// modifications later.
func (db *DocumentBundle) ForEachDocumentReadOnly(proc func(uint64, Document)) {
	db.RLock()
	defer db.RUnlock()
	pos := db.GetFirstDocOffset()
	for {
		if pos == 0 {
			return
		}
		doc := db.doGetDocumentAt(pos)
		proc(pos, doc)
		pos = doc.NextDocOffset
	}
}

// This version does not do its own locking, so we can support callers who already
// have the lock.
func (db *DocumentBundle) doPutDocument(doc Document) uint64 {

	lastDocOffset := db.GetLastDocOffset()

	//Handle case of initial insert
	if lastDocOffset == 0 {
		if 16+doc.byteSize() >= uint64(len(db.AsBytes)) {
			db.doGrowDB()
		}
		doc.PrevDocOffset = 0
		doc.NextDocOffset = 0
		db.setFirstDocOffset(16)
		db.setLastDocOffset(16)
		copy(db.AsBytes[16:], doc.toBytes())
		return 16
	}

	lastDoc := db.GetDocumentAt(lastDocOffset)
	//If not enough space, grow DB
	if !(lastDocOffset+lastDoc.byteSize()+doc.byteSize() >= uint64(len(db.AsBytes))) {
		db.doGrowDB()
	}
	//Adjust doc pointers
	doc.PrevDocOffset = lastDocOffset
	doc.NextDocOffset = 0
	//Appending
	insertPoint := lastDocOffset + lastDoc.byteSize()
	copy(db.AsBytes[insertPoint:], doc.toBytes())
	//Update the DB pointer to the last doc
	db.setLastDocOffset(insertPoint)
	//The now 2nd-to-last doc's pointer to the next doc
	db.setNextDocOffset(lastDocOffset, insertPoint)

	//Index
	db.indexDocument(doc, insertPoint)

	return insertPoint
}

// Insert the given (new) document and return the index at which it was inserted.
// Right now this always inserts at the end, but if we ever have a use pattern w/
// lots of removals / growing edits, we could do a malloc-tracking type thing
func (db *DocumentBundle) PutDocument(doc Document) uint64 {
	db.Lock()
	defer db.Unlock()
	return db.doPutDocument(doc)
}

// This version does not do its own locking, so we can support callers who already
// have the lock.
func (db *DocumentBundle) doRemoveDocumentAt(offset uint64) {
	targ := db.GetDocumentAt(offset)
	prevDocOffset := targ.PrevDocOffset
	nextDocOffset := targ.NextDocOffset

	db.setNextDocOffset(prevDocOffset, nextDocOffset)
	db.setPrevDocOffset(nextDocOffset, prevDocOffset)

	if db.GetFirstDocOffset() == offset {
		db.setFirstDocOffset(nextDocOffset)
	}
	if db.GetLastDocOffset() == offset {
		db.setLastDocOffset(prevDocOffset)
	}
	db.deindexDocument(targ, offset)
}

// Adjust the pointers to bypass the given document - does not zero the storage,
// but a compaction will result in it being overwritten.
func (db *DocumentBundle) RemoveDocumentAt(offset uint64) {
	db.Lock()
	defer db.Unlock()
	db.doRemoveDocumentAt(offset)
}

// Attempt to update the given document inplace - if it cannot be done, remove the
// existing document and insert the new one at the end
func (db *DocumentBundle) ReplaceDocument(offset uint64, newDoc Document) uint64 {
	db.Lock()
	defer db.Unlock()
	curDoc := db.GetDocumentAt(offset)
	if curDoc.NextDocOffset > newDoc.byteSize()+offset {
		db.deindexDocument(db.GetDocumentAt(offset), offset)
		copy(db.AsBytes[offset:], newDoc.toBytes())
		db.indexDocument(newDoc, offset)
	} else {
		//Indexing is handled by subroutines
		db.doRemoveDocumentAt(offset)
		return db.doPutDocument(newDoc)
	}
	return 0
}

// Concatenate all documents, adjust pointers, and truncate the file
func (db *DocumentBundle) Compact() {
	db.Lock()
	defer db.Unlock()

	//Clear indexes
	for _, idx := range db.indexes {
		idx.lookup = make(map[interface{}][]uint64, len(idx.lookup))
	}

	firstDocPos := db.GetFirstDocOffset()
	firstDoc := db.GetDocumentAt(firstDocPos)
	nextDocPos := firstDoc.NextDocOffset

	//Handle moving initial document to head if necessary
	if firstDocPos != 16 {
		copy(db.AsBytes[16:], firstDoc.toBytes())
		db.setFirstDocOffset(16)
		db.setPrevDocOffset(nextDocPos, 16)

		//Update index
		for _, idx := range db.indexes {
			key := idx.keyFn(firstDoc.Payload)
			idx.lookup[key] = append(idx.lookup[key], 16)
		}
		//Necessary to set last-doc pointer if we have only one doc
		firstDocPos = 16
	}

	for nextDocPos != 0 {
		nextDoc := db.GetDocumentAt(nextDocPos)
		insertPoint := firstDocPos + uint64(firstDoc.Size)

		//Move nextDoc to end of firstDoc, point nextDoc and firstDoc at each other
		copy(db.AsBytes[insertPoint:], nextDoc.toBytes())
		db.setNextDocOffset(firstDocPos, insertPoint)
		db.setPrevDocOffset(insertPoint, nextDocPos)

		//Update index
		for _, idx := range db.indexes {
			key := idx.keyFn(nextDoc.Payload)
			idx.lookup[key] = append(idx.lookup[key], insertPoint)
		}

		//Move up cursor
		firstDocPos = insertPoint
		nextDocPos = nextDoc.NextDocOffset
		firstDoc = nextDoc
	}

	db.setLastDocOffset(firstDocPos)

	newSize := firstDocPos + uint64(firstDoc.Size)
	//Now shrink the underlying file & remap the mmap
	syscall.Munmap(db.AsBytes)
	newFile, _ := os.OpenFile(db.FileLoc, os.O_RDWR|os.O_CREATE, 0666)
	defer newFile.Close()
	newFile.Truncate(int64(newSize))
	newArr, _ := syscall.Mmap(int(newFile.Fd()), 0, int(newSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	db.AsBytes = newArr
}
