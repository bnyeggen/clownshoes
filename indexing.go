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
