package boltseq

import (
	"bytes"

	"github.com/boltdb/bolt"
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

func (c *Cursor) First() bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.First())
}

func (c *Cursor) Last() bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.Last())
}

func (c *Cursor) Next() bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.Next())
}

func (c *Cursor) Prev() bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.Prev())
}

func (c *Cursor) Seek(seq uint64) bool {
	if c.cs == nil {
		return false
	}

	return c.sync(c.cs.Seek((newValue(seq, nil).seqBytes())))
}

func (c *Cursor) Err() error {
	return c.err
}

func (c *Cursor) Seq() uint64 {
	return c.seq
}

func (c *Cursor) Key() []byte {
	return c.key
}

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

func (c *Cursor) Delete() error {
	err := c.dp.Delete(c.key)
	if err != nil {
		return err
	}

	return c.cs.Delete()
}
