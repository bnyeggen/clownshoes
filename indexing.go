package clownshoes

import (
	"bufio"
	"os"
)

//Indexes have to be in memory for performance anyway, so we store them as
//hashmaps.  Equality only.  And, they aren't persistent - you have to re-add them
//(& recalculate) on startup.
type index struct {
	keyFn  func([]byte) string //Derives the key from the document's data
	lookup map[string][]uint64 //Maintains the lookup from key value to a list of offsets
}

func (db *DocumentBundle) deindexDocument(doc Document, offset uint64) {
	for _, idx := range db.indexes {
		key := idx.keyFn(doc.Payload)
		arr := idx.lookup[key]
		for i := 0; i < len(arr); i++ {
			if arr[i] == offset {
				arr[i] = arr[len(arr)-1]
				idx.lookup[key] = arr[:len(arr)-1]
				break
			}
		}
	}
}

func (db *DocumentBundle) indexDocument(doc Document, insertPoint uint64) {
	for _, idx := range db.indexes {
		key := idx.keyFn(doc.Payload)
		idx.lookup[key] = append(idx.lookup[key], insertPoint)
	}
}

// Creates an index on the DB, with the given name, and the given function of the
// document's payload for determining the key.  Right now indexes exist
// transiently in memory, necessitating re-creation on each restart.  See README
// for notes on future plans for indexing.
func (db *DocumentBundle) AddIndex(indexName string, keyFn func([]byte) string) {
	//Prevents concurrent modifications to the indexes
	db.Lock()
	defer db.Unlock()
	idx := index{keyFn, make(map[string][]uint64)}
	db.indexes[indexName] = idx
	//Now calculate values by iterating thru maps
	db.doForEachDocument(func(offset uint64, doc Document) {
		idx.lookup[keyFn(doc.Payload)] = append(idx.lookup[keyFn(doc.Payload)], offset)
	})
}

func (db *DocumentBundle) RemoveIndex(indexName string) {
	db.Lock()
	defer db.Unlock()
	delete(db.indexes, indexName)
}

// Store the index to a file as a packed series of records.
// Each record is:
// An uint32 representing the number of bytes in the key value
// An uint32 representing the number of offsets
// The key value
// All of the offsets for that index name / key, as uint64s
func (db *DocumentBundle) DumpIndex(indexName string, outfile string) {
	f, _ := os.OpenFile(outfile, os.O_RDWR|os.O_CREATE, 0666)
	w := bufio.NewWriter(f)
	defer w.Flush()
	defer f.Close()

	buffer := make([]byte, 8)

	for key, positions := range db.indexes[indexName].lookup {
		keyAsBytes := []byte(key)
		uint32ToBytes(buffer, 0, uint32(len(keyAsBytes)))
		uint32ToBytes(buffer, 4, uint32(len(positions)))
		w.Write(buffer)
		w.Write(keyAsBytes)
		for _, position := range positions {
			uint64ToBytes(buffer, 0, position)
			w.Write(buffer[:8])
		}
	}
}

// Load the index to the given DB with the given name, and use the given keyFn
// to update it in the future.  Assumes the correctness of the index.
func (db *DocumentBundle) LoadIndex(keyFn func([]byte) string, indexName string, outfile string) (out index) {
	f, _ := os.Open(outfile)
	r := bufio.NewReader(f)
	defer f.Close()

	out.keyFn = keyFn

	buffer := make([]byte, 8)
	for {
		_, e := r.Read(buffer)
		if e != nil {
			return out
		}
		keyLength := uint32FromBytes(buffer, 0)
		nOffsets := uint32FromBytes(buffer, 4)

		keyBuffer := make([]byte, keyLength)
		r.Read(keyBuffer)

		offsetBuffer := make([]byte, nOffsets*8)
		r.Read(offsetBuffer)

		offsets := make([]uint64, nOffsets)
		for i := uint32(0); i < nOffsets; i++ {
			offsets[i] = uint64FromBytes(offsetBuffer, 0)
			offsetBuffer = offsetBuffer[8:]
		}
	}
}
