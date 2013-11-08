package clownshoes

//Indexes have to be in memory for performance anyway, so we store them as
//hashmaps.  Equality only.  And, they aren't persistent - you have to re-add them
//(& recalculate) on startup.
type index struct {
	keyFn  func([]byte) interface{} //Derives the key from the document's data
	lookup map[interface{}][]uint64 //Maintains the lookup from key value to a list of offsets
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
func (db *DocumentBundle) AddIndex(indexName string, keyFn func([]byte) interface{}) {
	//Prevents concurrent modifications to the indexes
	db.Lock()
	defer db.Unlock()
	idx := index{keyFn, make(map[interface{}][]uint64)}
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

// Store the index to a flatfile.
func (db *DocumentBundle) DumpIndex(indexName string, outfile string) {
	panic("Unimplemented")
}

// Load the index to the given DB with the given name, and use the given keyFn
// to update it in the future.  Assumes the correctness of the index.
func (db *DocumentBundle) LoadIndex(keyFn func([]byte) interface{}, indexName string, outfile string) {
	panic("Unimplemented")
}
