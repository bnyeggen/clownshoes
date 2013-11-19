package clownshoes

import (
	"encoding/gob"
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

// Store the indexes to a file as a packed series of index records.
// Each record is:
// A uint64 representing the total number of bytes in the record
// A uint32 representing the number of bytes in the index name
// The index name
// And then for each key / offset pair:
// An uint32 representing the number of bytes in the key
// An uint32 representing the number of bytes in the offsets (not the number of elements)
// The key value
// All of the offsets for that index name / key, as uint64s
func (db *DocumentBundle) DumpIndexes(outfile string) {
	f, _ := os.OpenFile(outfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	defer f.Close()
	outGobEncoder := gob.NewEncoder(f)
	out := make(map[string]map[string][]uint64)
	for idxname, idx := range db.indexes {
		out[idxname] = idx.lookup
	}
	outGobEncoder.Encode(out)
}

// Load the packed indexes from the given file, using the supplied map to associate
// the appropriate key function with them going forward.  Set the db's indexes as
// such.  Assumes the index is valid & up-to-date.
func (db *DocumentBundle) LoadIndexes(nameToKeyFns map[string]func([]byte) string, indexFile string) {
	f, _ := os.Open(indexFile)
	defer f.Close()
	inGobDecoder := gob.NewDecoder(f)

	data := make(map[string]map[string][]uint64)
	inGobDecoder.Decode(&data)

	for idxName, idxlookup := range data {
		db.indexes[idxName] = index{nameToKeyFns[idxName], idxlookup}
	}
}
