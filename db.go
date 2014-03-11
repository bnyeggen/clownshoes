package clownshoes

//Core data manipulation functions

import (
	"os"
	"reflect"
	"sync"
	"syscall"
	"unsafe"
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

//Abstracts writes to allow for transparent journaling.
func (db *DocumentBundle) writeBytes(pos uint64, data []byte) {
	copy(db.AsBytes[pos:], data)
}

func (db *DocumentBundle) writePointer(pos uint64, data uint64) {
	uint64ToBytes(db.AsBytes, pos, data)
}

// Return the offset of the first valid document in the DB, or 0 if there is none
func (db *DocumentBundle) getFirstDocOffset() uint64 {
	return uint64FromBytes(db.AsBytes, 0)
}

// Return the offset of the last valid document in the DB, or 0 if it is empty
func (db *DocumentBundle) getLastDocOffset() uint64 {
	return uint64FromBytes(db.AsBytes, 8)
}

func (db *DocumentBundle) setFirstDocOffset(offset uint64) {
	db.writePointer(0, offset)
}
func (db *DocumentBundle) setLastDocOffset(offset uint64) {
	db.writePointer(8, offset)
}

// For the document at the given position, update the pointer to the next document
func (db *DocumentBundle) setNextDocOffset(docOffset uint64, nextDocOffset uint64) {
	db.writePointer(docOffset+4, nextDocOffset)
}

// For the document at the given position, update the pointer to the previous document
func (db *DocumentBundle) setPrevDocOffset(docOffset uint64, prevDocOffset uint64) {
	db.writePointer(docOffset+4+8, prevDocOffset)
}

// Copies the data, overwriting if necessary, to a file at destination.  Calling
// this periodically is the only way to ensure you have a consistent version of
// your data.  "" as indexDest does not dump indexes.
func (db *DocumentBundle) CopyDB(dataDest, indexDest string) error {
	db.RLock()
	defer db.RUnlock()
	dataFile, err := os.Create(dataDest)
	if err != nil {
		return err
	}
	if indexDest != "" {
		err = db.dumpIndexes(indexDest)
		if err != nil {
			return err
		}
	}
	dataFile.Write(db.AsBytes)
	return dataFile.Close()
}

func MSync(m *[]byte) error {
	dh := (*reflect.SliceHeader)(unsafe.Pointer(m))
	addr := dh.Data
	mmap_len := uintptr(dh.Len)
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, addr, mmap_len, syscall.MS_SYNC)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

// Flush all writes to disk
func (db *DocumentBundle) Sync() error {
	db.Lock()
	defer db.Unlock()
	return MSync(&db.AsBytes)
}

// Grow or shrink the backing storage for the given db to the given size
func (db *DocumentBundle) doReMmap(size uint64) error {
	e := syscall.Munmap(db.AsBytes)
	if e != nil {
		return e
	}
	newFile, e := os.OpenFile(db.FileLoc, os.O_RDWR|os.O_CREATE, 0666)
	if e != nil {
		return e
	}
	e = newFile.Truncate(int64(size))
	if e != nil {
		return e
	}
	newArr, e := syscall.Mmap(int(newFile.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if e != nil {
		return e
	}
	db.AsBytes = newArr
	return newFile.Close()
}

// Concatenate all documents, adjust pointers to be consistent, re-index,
// and truncate the file.
func (db *DocumentBundle) Compact() {
	db.Lock()
	defer db.Unlock()

	idxNames := make([]string, 0)
	idxFns := make([]func([]byte) string, 0)
	//Record indexes for re-creation later
	for idxName, idx := range db.indexes {
		idxNames = append(idxNames, idxName)
		idxFns = append(idxFns, idx.keyFn)
	}

	firstDocPos := db.getFirstDocOffset()

	//Compacting an empty database to 4k - will still grow in 1gb chunks
	if firstDocPos == 0 {
		db.doReMmap(4096)
		return
	}

	firstDoc := db.doGetDocumentAt(firstDocPos)
	nextDocPos := firstDoc.NextDocOffset

	//Move initial document to the head if necessary
	if firstDocPos != 16 {
		db.writeBytes(16, firstDoc.toBytes())
		db.setFirstDocOffset(16)
		//Back-pointer handled below, if there is a next document to point back from

		firstDocPos = 16
	}

	for nextDocPos != 0 {
		nextDoc := db.doGetDocumentAt(nextDocPos)
		insertPoint := firstDocPos + firstDoc.byteSize()

		//Move nextDoc to end of firstDoc, point nextDoc and firstDoc at each other
		db.writeBytes(insertPoint, nextDoc.toBytes())
		db.setNextDocOffset(firstDocPos, insertPoint)
		db.setPrevDocOffset(insertPoint, firstDocPos)

		//Move up cursor
		firstDocPos = insertPoint
		nextDocPos = nextDoc.NextDocOffset
		firstDoc = nextDoc
	}

	//nextDocPos is invalid at this point, so this is the final valid position
	db.setLastDocOffset(firstDocPos)
	newSize := firstDocPos + uint64(firstDoc.Size)

	//Now shrink the underlying file & re-mmap
	db.doReMmap(newSize)

	//And re-add indexes, which blows away the old values
	for i := 0; i < len(idxNames); i++ {
		db.doAddIndex(idxNames[0], idxFns[0])
	}
}

// Returns a new 1gb DocumentBundle, or loads the DocumentBundle at that location
// if it already exists.
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
	stats, _ = fileOut.Stat()

	var bytesOut []byte
	bytesOut, _ = syscall.Mmap(int(fileOut.Fd()), 0, int(stats.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return &DocumentBundle{sync.RWMutex{}, bytesOut, location, make(map[string]index, 0)}
}

// Grow the db's backing storage by 1gb.
func (db *DocumentBundle) doGrowDB() {
	oldSize := uint64(len(db.AsBytes))
	db.doReMmap(oldSize + 1000000000)
}

// Run the given function sequentially over each valid document.
// We may support different contracts wrt locking and concurrent execution or
// modifications later, but right now this is guaranteed not to process
// concurrently, and assumes the caller already holds some kind of lock.
func (db *DocumentBundle) doForEachDocument(proc func(uint64, Document)) {
	pos := db.getFirstDocOffset()
	for pos != 0 {
		doc := db.doGetDocumentAt(pos)
		proc(pos, doc)
		pos = doc.NextDocOffset
	}
}

// Without acquiring the lock (assumes the caller already holds it), insert the
// given document at the end of the DB.
func (db *DocumentBundle) doPutDocument(doc Document) uint64 {
	lastDocOffset := db.getLastDocOffset()

	//Handle case of initial insert
	if lastDocOffset == 0 {
		if 16+doc.byteSize() >= uint64(len(db.AsBytes)) {
			db.doGrowDB()
		}
		doc.PrevDocOffset = 0
		doc.NextDocOffset = 0
		db.setFirstDocOffset(16)
		db.setLastDocOffset(16)
		db.writeBytes(16, doc.toBytes())
		db.indexDocument(doc, 16)
		return 16
	}

	lastDoc := db.doGetDocumentAt(lastDocOffset)
	//If not enough space, grow DB
	if lastDocOffset+lastDoc.byteSize()+doc.byteSize() >= uint64(len(db.AsBytes)) {
		db.doGrowDB()
	}
	//Adjust doc pointers
	doc.PrevDocOffset = lastDocOffset
	doc.NextDocOffset = 0
	//Appending
	insertPoint := lastDocOffset + lastDoc.byteSize()
	db.writeBytes(insertPoint, doc.toBytes())
	//Update the DB pointer to the last doc
	db.setLastDocOffset(insertPoint)
	//The now 2nd-to-last doc's pointer to the next doc
	db.setNextDocOffset(lastDocOffset, insertPoint)

	//Index
	db.indexDocument(doc, insertPoint)

	return insertPoint
}

// Adjust the pointers to bypass the given document - does not zero the storage,
// but a compaction will result in it being overwritten.
func (db *DocumentBundle) doRemoveDocumentAt(offset uint64) {
	targ := db.doGetDocumentAt(offset)
	prevDocOffset := targ.PrevDocOffset
	nextDocOffset := targ.NextDocOffset

	//Not first document
	if prevDocOffset != 0 {
		db.setNextDocOffset(prevDocOffset, nextDocOffset)
	} else {
		db.setFirstDocOffset(nextDocOffset)
	}
	//Not last document
	if nextDocOffset != 0 {
		db.setPrevDocOffset(nextDocOffset, prevDocOffset)
	} else {
		db.setLastDocOffset(prevDocOffset)
	}

	db.deindexDocument(targ, offset)
}

// Attempt to update the given document inplace - if it cannot be done, remove the
// existing document and insert the new one at the end
func (db *DocumentBundle) doReplaceDocument(offset uint64, newDoc Document) uint64 {
	curDoc := db.doGetDocumentAt(offset)
	if newDoc.byteSize()+offset < curDoc.NextDocOffset {
		db.deindexDocument(db.doGetDocumentAt(offset), offset)
		newDoc.NextDocOffset = curDoc.NextDocOffset
		newDoc.PrevDocOffset = curDoc.PrevDocOffset
		db.writeBytes(offset, newDoc.toBytes())
		db.indexDocument(newDoc, offset)
		return offset
	} else {
		//Indexing and modifying offsets is handled by subroutines
		db.doRemoveDocumentAt(offset)
		return db.doPutDocument(newDoc)
	}
}
