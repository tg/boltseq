// boltseq allows for creating a bolt bucket which tracks order of items added to it.
// Every key is given sequence ID, which is incremented for every insertion/modification.
// Items can be accessed by key or sequence number.
package boltseq

import (
	"encoding/binary"
	"errors"

	bolt "go.etcd.io/bbolt"
)

// sub-bucket names
var (
	bucketNameData = []byte("data")
	bucketNameSeq  = []byte("seq")
)

// Location is implemented by bolt.Tx and bolt.Bucket
type Location interface {
	Bucket(name []byte) *bolt.Bucket
	CreateBucket(key []byte) (*bolt.Bucket, error)
	CreateBucketIfNotExists(key []byte) (*bolt.Bucket, error)
}

// DataBucket returns bucket
func DataBucket(b Location) *bolt.Bucket {
	return b.Bucket(bucketNameData)
}

// Value is a value for a key stored in the bucket.
// It consists of sequence number and data.
type Value []byte

func newValue(seq uint64, val []byte) Value {
	v := make([]byte, 8+len(val))
	binary.BigEndian.PutUint64(v[:8], seq)
	copy(v[8:], val)
	return v
}

// IsValid tells whether value is valid.
func (v Value) IsValid() bool {
	return len(v) >= 8
}

// Data returns data part of the value.
func (v Value) Data() []byte {
	return v[8:]
}

// Seq returns seequence number of the value.
func (v Value) Seq() uint64 {
	return binary.BigEndian.Uint64(v[:8])
}

func (v Value) seqBytes() []byte {
	return v[:8]
}

// Bucket reporesents boltseq.Bucket at given location.
type Bucket struct {
	loc Location
}

// NewBucket creates a boltseq bucket at given location.
// This call has no side-effects, in particular sub-buckets are created on Put.
func NewBucket(loc Location) *Bucket {
	return &Bucket{loc: loc}
}

var (
	ErrInvalidValue  = errors.New("invalid value")
	ErrInvalidBucket = errors.New("invalid bucket")
	ErrInvalidKey    = errors.New("invalid key")
)

// Put adds key-value pair into the bucket. Returns sequence number and error, if any.
// The key is always given a new sequence number, even if it already exists.
func (b *Bucket) Put(key []byte, value []byte) (uint64, error) {
	bd, err := b.loc.CreateBucketIfNotExists(bucketNameData)
	if err != nil {
		return 0, err
	}

	bs, err := b.loc.CreateBucketIfNotExists(bucketNameSeq)
	if err != nil {
		return 0, err
	}

	// Delete current value
	if v := Value(bd.Get(key)); v != nil {
		if !v.IsValid() {
			return 0, ErrInvalidValue
		}
		if err := bs.Delete(v.seqBytes()); err != nil {
			return 0, err
		}
		if err := bd.Delete(key); err != nil {
			return 0, err
		}
	}

	// Get next sequence
	seq, err := bs.NextSequence()
	if err != nil {
		return seq, err
	}

	// Make value
	val := newValue(seq, value)

	// Add seq->key mapping. Fill percent is set to 100% as
	// we add keys in order.
	bs.FillPercent = 1
	if err := bs.Put(val.seqBytes(), key); err != nil {
		return seq, err
	}

	return seq, bd.Put(key, val)
}

// Get returns Value for the key
func (b *Bucket) Get(key []byte) Value {
	bd := b.loc.Bucket(bucketNameData)
	if bd == nil {
		return nil
	}
	return Value(bd.Get(key))
}

// GetSeq returns data value for a key with sequence number `seq`
func (b *Bucket) GetSeq(seq uint64) []byte {
	bs := b.loc.Bucket(bucketNameSeq)
	if bs == nil {
		return nil
	}
	return bs.Get(newValue(seq, nil).seqBytes())
}

// Delete deletes a key
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

// DeleteSeq deletes a key with sequence number `seq`
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

// Cursor returns iterator over the bucket
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
