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

// Store the indexes to a file. This is private because for consistency it should
// always happen in the context of CopyDB
func (db *DocumentBundle) dumpIndexes(outfile string) error {
	f, e := os.OpenFile(outfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if e != nil {
		return e
	}
	defer f.Close()
	outGobEncoder := gob.NewEncoder(f)
	out := make(map[string]map[string][]uint64)
	for idxname, idx := range db.indexes {
		out[idxname] = idx.lookup
	}
	e = outGobEncoder.Encode(out)
	return e
}

// Load the packed indexes from the given file, using the supplied map to associate
// the appropriate key function with them going forward.  Set the db's indexes as
// such.  Assumes the index is valid & up-to-date.
func (db *DocumentBundle) LoadIndexes(nameToKeyFns map[string]func([]byte) string, indexFile string) {
	db.Lock()
	f, _ := os.Open(indexFile)
	defer f.Close()
	defer db.Unlock()
	inGobDecoder := gob.NewDecoder(f)

	data := make(map[string]map[string][]uint64)
	inGobDecoder.Decode(&data)

	for idxName, idxlookup := range data {
		db.indexes[idxName] = index{nameToKeyFns[idxName], idxlookup}
	}
}
