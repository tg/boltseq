package boltseq

import (
	"bytes"

	bolt "github.com/etcd-io/bbolt"
)

type pointer struct {
	c   *bolt.Cursor
	key []byte
	val []byte
}

func (p *pointer) move(k []byte) bool {
	if p.c == nil {
		return false
	}
	if bytes.Equal(p.key, k) {
		return true
	}
	p.key, p.val = p.c.Seek(k)
	return bytes.Equal(p.key, k)
}

func (p *pointer) Get(k []byte) (val []byte, ok bool) {
	if ok = p.move(k); ok {
		val = p.val
	}
	return
}

func (p *pointer) Delete(k []byte) error {
	if !p.move(k) {
		return nil
	}
	return p.c.Delete()
}

// Cursors allows for iterating buckets according to sequence number.
type Cursor struct {
	cs *bolt.Cursor
	dp pointer

	seq uint64
	key []byte

	err error
}

func (c *Cursor) sync(seq []byte, key []byte) bool {
	if seq == nil {
		return false
	}

	v := Value(seq)
	if !v.IsValid() {
		c.err = ErrInvalidKey
		return false
	}

	c.seq = v.Seq()
	c.key = key
	return true
}

// First moves cursor to the first key/value pair.
// Returns false on empty bucket, true otherwise.
func (c *Cursor) First() bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.First())
}

// Last moves cursor to the last key/value pair.
// Returns false on empty bucket, true otherwise.
func (c *Cursor) Last() bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.Last())
}

// Next moves cursor to the next key/value pair.
// Returns false is reached end of the bucket, true otherwise.
func (c *Cursor) Next() bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.Next())
}

// Prev moves cursor to the previous key/value pair.
// Returns false is reached end of the bucket, true otherwise.
func (c *Cursor) Prev() bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.Prev())
}

// Seek moves cursor to the key/value pair at the given seq number.
// If seq number doesn't exists it points to the next item, if any.
// Returns false if no item, true otherwise.
func (c *Cursor) Seek(seq uint64) bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.Seek((newValue(seq, nil).seqBytes())))
}

// Err returns error, if any.
func (c *Cursor) Err() error {
	return c.err
}

// Seq returns current sequence number.
func (c *Cursor) Seq() uint64 {
	return c.seq
}

// Key returns current key.
func (c *Cursor) Key() []byte {
	return c.key
}

// Data returns current data for the key.
func (c *Cursor) Data() ([]byte, error) {
	v, ok := c.dp.Get(c.key)
	if !ok {
		return nil, ErrInvalidKey
	}

	val := Value(v)
	if !val.IsValid() {
		return nil, ErrInvalidValue
	}

	return val.Data(), nil
}

// Delete deletes the current item.
func (c *Cursor) Delete() error {
	err := c.dp.Delete(c.key)
	if err != nil {
		return err
	}

	return c.cs.Delete()
}
