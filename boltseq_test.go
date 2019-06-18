package boltseq

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	bolt "github.com/etcd-io/bbolt"
)

var testBucketName = []byte("test")

func newTestDB() (*bolt.DB, error) {
	f, err := ioutil.TempFile("", "boltseq_test")
	if err != nil {
		return nil, err
	}

	db, err := bolt.Open(f.Name(), 0600, nil)
	if err != nil {
		os.Remove(f.Name())
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(testBucketName)
		return err
	})
	if err != nil {
		os.Remove(f.Name())
		return nil, err
	}

	return db, nil
}

func TestBucket_nx(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(db.Path())

	err = db.View(func(tx *bolt.Tx) error {
		b := NewBucket(tx.Bucket(testBucketName))
		v := b.Get([]byte("nx"))
		if v != nil {
			t.Fatal(v)
		}
		return nil
	})
}

func TestBucket_put(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(db.Path())

	err = db.Update(func(tx *bolt.Tx) error {
		b := NewBucket(tx.Bucket(testBucketName))

		seq, err := b.Put([]byte("x"), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		if seq != 1 {
			t.Fatal(seq)
		}

		v := b.Get([]byte("x"))
		if v == nil || !v.IsValid() {
			t.Fatal(v)
		}
		if seq := v.Seq(); seq != 1 {
			t.Fatal(seq)
		}
		if d := v.Data(); len(d) != 1 || d[0] != 'v' {
			t.Fatal(d)
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestBucket_putMany(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(db.Path())

	keys := make([]string, 1000)
	for n := range keys {
		h := sha1.Sum([]byte(fmt.Sprint(n)))
		keys[n] = string(h[:])
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b := NewBucket(tx.Bucket(testBucketName))

		for n, key := range keys {
			seq, err := b.Put([]byte(key), []byte(fmt.Sprint(n+1)))
			if err != nil {
				t.Fatal(err)
			}
			if seq != uint64(n+1) {
				t.Fatal(seq, n)
			}
		}

		// Get and check every value
		for n, key := range keys {
			seq := uint64(n + 1)

			// Check GetSeq
			if k := b.GetSeq(seq); string(k) != string(key) {
				t.Fatalf("%s != %s", k, key)
			}

			// Check Get
			if v := b.Get([]byte(key)); !v.IsValid() || v.Seq() != seq || string(v.Data()) != fmt.Sprint(seq) {
				t.Fatal(key, v)
			}
		}

		// Travel via cursor
		c := b.Cursor()
		n := 0
		for ok := c.First(); ok; ok = c.Next() {
			seq := uint64(n + 1)

			if c.Seq() != seq {
				t.Fatal(seq, c.Seq())
			}
			if k := string(c.Key()); k != keys[n] {
				t.Fatal(seq, k)
			}
			if v, err := c.Data(); err != nil || string(v) != fmt.Sprint(seq) {
				t.Fatal(seq, string(v))
			}

			n++
		}
		if err := c.Err(); err != nil {
			t.Fatal(err)
		}
		if n != len(keys) {
			t.Fatal(n)
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}
