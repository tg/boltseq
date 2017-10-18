package boltseq

import (
	"encoding/binary"
	"errors"

	"github.com/boltdb/bolt"
)

var (
	bucketNameData = []byte("data")
	bucketNameSeq  = []byte("seq")
)

type Location interface {
	Bucket(name []byte) *bolt.Bucket
	CreateBucket(key []byte) (*bolt.Bucket, error)
	CreateBucketIfNotExists(key []byte) (*bolt.Bucket, error)
}

// DataBucket returns bucket
func DataBucket(b Location) *bolt.Bucket {
	return b.Bucket(bucketNameData)
}

type Value []byte

func newValue(seq uint64, val []byte) Value {
	v := make([]byte, 8+len(val))
	binary.BigEndian.PutUint64(v[:8], seq)
	copy(v[8:], val)
	return v
}

func (v Value) IsValid() bool {
	return len(v) >= 8
}

func (v Value) Data() []byte {
	return v[8:]
}

func (v Value) Seq() uint64 {
	return binary.BigEndian.Uint64(v[:8])
}

func (v Value) seqBytes() []byte {
	return v[:8]
}

type Bucket struct {
	loc Location
}

func NewBucket(loc Location) *Bucket {
	return &Bucket{loc: loc}
}

var (
	ErrInvalidValue  = errors.New("invalid value")
	ErrInvalidBucket = errors.New("invalid bucket")
	ErrInvalidKey    = errors.New("invalid key")
)

func (b *Bucket) Put(key []byte, value []byte) error {
	bd, err := b.loc.CreateBucketIfNotExists(bucketNameData)
	if err != nil {
		return err
	}

	bs, err := b.loc.CreateBucketIfNotExists(bucketNameSeq)
	if err != nil {
		return err
	}

	// Delete current value
	if v := Value(bd.Get(key)); v != nil {
		if !v.IsValid() {
			return ErrInvalidValue
		}
		if err := bs.Delete(v.seqBytes()); err != nil {
			return err
		}
		if err := bd.Delete(key); err != nil {
			return err
		}
	}

	// Get next sequence
	seq, err := bs.NextSequence()
	if err != nil {
		return err
	}

	// Make value
	val := newValue(seq, value)

	// Add seq->key mapping. Fill percent is set to 100% as
	// we add keys in order.
	bs.FillPercent = 1
	if err := bs.Put(val.seqBytes(), key); err != nil {
		return err
	}

	return bd.Put(key, val)
}

func (b *Bucket) Get(key []byte) Value {
	bd := b.loc.Bucket(bucketNameData)
	if bd == nil {
		return nil
	}
	return Value(bd.Get(key))
}

func (b *Bucket) GetSeq(seq uint64) []byte {
	bs := b.loc.Bucket(bucketNameSeq)
	if bs == nil {
		return nil
	}
	return bs.Get(newValue(seq, nil).seqBytes())
}

func (b *Bucket) Delete(key []byte) error {
	bd := b.loc.Bucket(bucketNameData)
	if bd == nil {
		return ErrInvalidBucket
	}

	v := Value(bd.Get(key))
	if v == nil {
		return nil
	}
	if !v.IsValid() {
		return ErrInvalidValue
	}

	bs := b.loc.Bucket(bucketNameSeq)
	if bs == nil {
		return ErrInvalidBucket
	}

	if err := bs.Delete(v.seqBytes()); err != nil {
		return err
	}

	return bd.Delete(key)
}

func (b *Bucket) DeleteSeq(seq uint64) error {
	c := b.Cursor()
	if !c.Seek(seq) {
		return c.Err()
	}
	if c.Seq() != seq {
		return nil
	}

	return c.Delete()
}

func (b *Bucket) Cursor() *Cursor {
	var cs, cd *bolt.Cursor

	bs := b.loc.Bucket(bucketNameSeq)
	if bs != nil {
		cs = bs.Cursor()
	}
	bd := b.loc.Bucket(bucketNameData)
	if bd != nil {
		cd = bd.Cursor()
	}

	return &Cursor{
		cs: cs,
		dp: pointer{c: cd},
	}
}
