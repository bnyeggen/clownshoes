package clownshoes

// Publicly facing higher-order modification functions

// Using the index with the given name, look up all the documents with the
// given key and return them.
func (db *DocumentBundle) GetDocumentsWhere(indexName string, lookupKey string) (docs []Document) {
	db.RLock()
	defer db.RUnlock()
	idx, found := db.indexes[indexName]
	if found {
		offsets := idx.lookup[lookupKey]
		for _, offset := range offsets {
			docs = append(docs, db.doGetDocumentAt(offset))
		}
	}
	return docs
}

// Return all the documents for which the given function returns true, scanning
// the DB to do so.
func (db *DocumentBundle) GetDocuments(filter func([]byte) bool) (docs []Document) {
	db.RLock()
	defer db.RUnlock()

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
func (db *DocumentBundle) ReplaceDocumentsWhere(indexName string, lookupKey string, replacer func([]byte) ([]byte, bool)) (counter uint64) {
	db.Lock()
	defer db.Unlock()

	idx, found := db.indexes[indexName]
	if found {
		offsets := idx.lookup[lookupKey]
		for _, offset := range offsets {
			newPayload, modified := replacer(db.doGetDocumentAt(offset).Payload)
			if modified {
				db.doReplaceDocument(offset, NewDocument(newPayload))
				counter++
			}
		}
	}
	return counter
}

// For all valid documents, if the second return value of the replacer function
// ran over the payload is true, replace the payload with the first return
// value. Returns the number of documents affected.
func (db *DocumentBundle) ReplaceDocuments(replacer func([]byte) ([]byte, bool)) (counter uint64) {
	db.Lock()
	defer db.Unlock()

	//This traverses in reverse to avoid an infinite loop with modifications that
	//expand documents, which results in an insert at the end of the file.
	pos := db.getLastDocOffset()
	for pos != 0 {
		curDoc := db.doGetDocumentAt(pos)
		newPayload, modified := replacer(curDoc.Payload)
		if modified {
			db.doReplaceDocument(pos, NewDocument(newPayload))
			counter++
		}
		pos = curDoc.PrevDocOffset
	}
	return counter
}

// Using the index with the given name, remove all documents with the given key
// and where the supplied function of the payload returns true.  Returns the
// number of documents affected.
func (db *DocumentBundle) RemoveDocumentsWhere(indexName string, lookupKey string, filter func([]byte) bool) (counter uint64) {
	db.Lock()
	defer db.Unlock()

	idx, found := db.indexes[indexName]
	if found {
		offsets := idx.lookup[lookupKey]
		for _, offset := range offsets {
			if filter(db.doGetDocumentAt(offset).Payload) {
				db.doRemoveDocumentAt(offset)
				counter++
			}
		}
	}
	return counter
}

// Remove all documents where the supplied function of their payloads returns true.
// Scans the whole DB and returns the number of documents affected.
func (db *DocumentBundle) RemoveDocuments(filter func([]byte) bool) (counter uint64) {
	db.Lock()
	defer db.Unlock()

	db.doForEachDocument(func(offset uint64, doc Document) {
		if filter(doc.Payload) {
			db.doRemoveDocumentAt(offset)
			counter++
		}
	})

	return counter
}

// Insert the given (new) document and return the index at which it was inserted.
// Right now this always inserts at the end, but if we ever have a use pattern w/
// lots of removals / growing edits, we could do a malloc-tracking type thing
func (db *DocumentBundle) PutDocument(doc Document) uint64 {
	db.Lock()
	defer db.Unlock()
	return db.doPutDocument(doc)
}
