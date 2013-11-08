package clownshoes

// Publicly facing higher-order modification functions

// Using the index with the given name, lookup all the documents with the given key and return them.
func (db *DocumentBundle) GetDocumentsWhere(indexName string, lookupKey interface{}) []Document {
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

// Return all the documents for which the given function returns true
func (db *DocumentBundle) GetDocuments(filter func([]byte) bool) []Document {
	db.RLock()
	defer db.RUnlock()

	docs := make([]Document, 0)

	db.doForEachDocument(func(offset uint64, doc Document) {
		if filter(doc.Payload) {
			docs = append(docs, doc)
		}
	})

	return docs
}

// Using the index, run the replacer function on all the documents with the given
// key.  If the second return value of the replacer function is true, replace the
// document with the first return value.  Returns the number of documents affected.
func (db *DocumentBundle) ReplaceDocumentsWhere(indexName string, lookupKey interface{}, replacer func([]byte) ([]byte, bool)) uint64 {
	db.Lock()
	defer db.Unlock()

	idx, found := db.indexes[indexName]
	if found {
		offsets := idx.lookup[lookupKey]
		ctr := uint64(0)
		for _, offset := range offsets {
			newPayload, modified := replacer(db.doGetDocumentAt(offset).Payload)
			if modified {
				db.doReplaceDocument(offset, NewDocument(newPayload))
				ctr++
			}
		}
		return ctr
	}
	return 0
}

// For all valid documents, if the second return value of the replacer function
// ran over the payload is true, replace the payload with the first return
// value. Returns the number of documents affected.
func (db *DocumentBundle) ReplaceDocuments(replacer func([]byte) ([]byte, bool)) uint64 {
	db.Lock()
	defer db.Unlock()

	ctr := uint64(0)

	db.doForEachDocument(func(offset uint64, doc Document) {
		newPayload, modified := replacer(doc.Payload)
		if modified {
			db.doReplaceDocument(offset, NewDocument(newPayload))
			ctr++
		}
	})

	return ctr
}

// Using the index with the given name, remove all documents with the given key
// and where the supplied function of the payload returns true.  Returns the
// number of documents affected.
func (db *DocumentBundle) RemoveDocumentsWhere(indexName string, lookupKey interface{}, filter func([]byte) bool) uint64 {
	db.Lock()
	defer db.Unlock()

	idx, found := db.indexes[indexName]
	if found {
		offsets := idx.lookup[lookupKey]
		ctr := uint64(0)
		for _, offset := range offsets {
			if filter(db.doGetDocumentAt(offset).Payload) {
				db.doRemoveDocumentAt(offset)
				ctr++
			}
		}
		return ctr
	}
	return 0
}

// Remove all documents where the supplied function of their payloads returns true.
// Returns the number of documents affected.
func (db *DocumentBundle) RemoveDocuments(filter func([]byte) bool) uint64 {
	db.Lock()
	defer db.Unlock()

	ctr := uint64(0)

	db.doForEachDocument(func(offset uint64, doc Document) {
		if filter(doc.Payload) {
			db.doRemoveDocumentAt(offset)
			ctr++
		}
	})

	return ctr
}

// Insert the given (new) document and return the index at which it was inserted.
// Right now this always inserts at the end, but if we ever have a use pattern w/
// lots of removals / growing edits, we could do a malloc-tracking type thing
func (db *DocumentBundle) PutDocument(doc Document) uint64 {
	db.Lock()
	defer db.Unlock()
	return db.doPutDocument(doc)
}
