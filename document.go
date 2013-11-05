package clownshoes

// Packed size in bytes of all elements of a Document save the Payload.
const docHeaderSize = 20

type Document struct {
	Size          uint32 //Number of bytes for the entire packed document, including this size & NextDocOffset
	NextDocOffset uint64 //Offset of the next valid document
	PrevDocOffset uint64 //Offset of previous valid document
	Payload       []byte //Your precious data
}

// Returns an empty document, ripe for insertion, with the given payload
func NewDocument(payload []byte) Document {
	return Document{docHeaderSize + uint32(len(payload)), 0, 0, payload}
}

// Total packed size of a document, returned as a uint64 but always in the uint32
// range.
func (doc *Document) byteSize() uint64 {
	return uint64(len(doc.Payload)) + docHeaderSize
}

// Return a serialized byte array representing a Document
func (doc *Document) toBytes() []byte {
	//Don't need to rely on stated size when serializing
	byteSize := doc.byteSize()
	out := make([]byte, byteSize)
	uint32ToBytes(out, 0, uint32(byteSize))
	uint64ToBytes(out, 4, doc.NextDocOffset)
	uint64ToBytes(out, 12, doc.PrevDocOffset)
	copy(out[docHeaderSize:], doc.Payload)
	return out
}

// This version does not do its own locking, so we can support callers who already
// have the lock.
func (db *DocumentBundle) doGetDocumentAt(offset uint64) Document {
	docLength := uint32FromBytes(db.AsBytes, offset)
	nextDocPos := uint64FromBytes(db.AsBytes, offset+4)
	prevDocPos := uint64FromBytes(db.AsBytes, offset+12)
	return Document{docLength, nextDocPos, prevDocPos, db.AsBytes[offset+docHeaderSize : offset+uint64(docLength)]}
}
