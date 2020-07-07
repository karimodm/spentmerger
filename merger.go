package main

import (
	"fmt"
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

type merger struct{}

func (m merger) Write(p []byte) (int, error) {
	fmt.Printf("Received: %s", p)
	os.Exit(1)
	return len(p), nil
}

func main() {

	argc := len(os.Args)
	argv := os.Args

	if argc != 5 {
		panic("Need to specify 3 params: BASEDIRECTORY DB1 DB2 MERGEDDB")
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
		b := tx.Bucket([]byte{storePrefixSpentAddresses})
		b.ForEach(func(k, v []byte) error {
			mergeddb.Update(func(tx *bbolt.Tx) error {
				_b := tx.Bucket([]byte{storePrefixSpentAddresses})
				_b.Put(k, v)
				return nil
			})
			return nil
		})
		return nil
	})

	/*
		db1store := bolt.New(db1)
		db2store := bolt.New(db2)
		dbmergedstore := bolt.New(mergeddb)

		d1 := configureSpentAddressesStorage(db1store)
		d2 := configureSpentAddressesStorage(db2store)
		m := configureSpentAddressesStorage(dbmergedstore)

		count := 0

		fmt.Println("Processing DB1")
		d1.ForEach(func(key []byte, c objectstorage.CachedObject) bool {
			o, stored := m.StoreIfAbsent(c.Get())
			c.Release(true)
			if stored {
				o.Release(true)
				count++
				if count%1000 == 0 {
					fmt.Printf("Merged %d addresses\n", count)
				}
			}
			return true
		})
		shutdownSpentAddressesStorage(d1)
		fmt.Println("DB1 processed.")

		fmt.Println("Processing DB2")
		d2.ForEach(func(key []byte, c objectstorage.CachedObject) bool {
			o, stored := m.StoreIfAbsent(c.Get())
			c.Release(true)
			if stored {
				o.Release(true)
				count++
				if count%1000 == 0 {
					fmt.Printf("Merged %d addresses\n", count)
				}
			}
			return true
		})
		fmt.Println("DB2 Processed.")

		shutdownSpentAddressesStorage(d2)
		shutdownSpentAddressesStorage(m)
	*/

}
