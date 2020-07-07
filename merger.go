package main

import (
	"os"

	"github.com/iotaledger/hive.go/kvstore/bolt"
	"go.etcd.io/bbolt"
)

func boltDB(directory string, filename string) *bbolt.DB {
	opts := &bbolt.Options{
		NoSync: true,
	}
	db, err := bolt.CreateDB(directory, filename, opts)
	if err != nil {
		panic(err)
	}
	return db
}

func main() {

	argc := len(os.Args)
	argv := os.Args

	if argc != 5 {
		panic("Need to specify 4 params: BASEDIRECTORY DB1 DB2 MERGEDDB")
	}

	directory := argv[1]
	pathdb1 := argv[2]
	pathdb2 := argv[3]
	pathmergeddb := argv[4]

	db1 := boltDB(directory, pathdb1)
	db2 := boltDB(directory, pathdb2)
	mergeddb := boltDB(directory, pathmergeddb)

	defer db1.Close()
	defer db2.Close()
	defer mergeddb.Close()

	bucket := []byte{storePrefixSpentAddresses}

	db1.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		b.ForEach(func(k, v []byte) error {
			mergeddb.Update(func(tx *bbolt.Tx) error {
				tx.CreateBucketIfNotExists(bucket)
				_b := tx.Bucket(bucket)
				_b.Put(k, v)
				return nil
			})
			return nil
		})
		return nil
	})

	db2.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		b.ForEach(func(k, v []byte) error {
			mergeddb.Update(func(tx *bbolt.Tx) error {
				_b := tx.Bucket(bucket)
				_b.Put(k, v)
				return nil
			})
			return nil
		})
		return nil
	})

}
