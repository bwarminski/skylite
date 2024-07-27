package main

import (
	"fmt"
	"log"
	"os"

	bolt "go.etcd.io/bbolt"
)

func main() {
	// Define the path for the test database
	dbPath := "test.db"

	// Ensure the database file does not already exist
	os.Remove(dbPath)

	// Open the BoltDB database
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		log.Fatalf("Failed to open BoltDB: %v", err)
	}
	defer db.Close()

	// Create a bucket and write data
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("TestBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		// Write a key-value pair
		err = bucket.Put([]byte("key"), []byte("value"))
		if err != nil {
			return fmt.Errorf("put key-value pair: %s", err)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}

	// Read the value back
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("TestBucket"))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		value := bucket.Get([]byte("key"))
		if value == nil {
			return fmt.Errorf("value not found")
		}

		fmt.Printf("Read value: %s\n", value)
		return nil
	})

	if err != nil {
		log.Fatalf("Read transaction failed: %v", err)
	}

	// Cleanup: remove the test database file
	err = os.Remove(dbPath)
	if err != nil {
		log.Fatalf("Failed to remove test database file: %v", err)
	}
}
