package main

import (
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	log.SetFlags(0)
	db, err := leveldb.OpenFile("/tmp/leveldb", nil)
	if err != nil {
		fmt.Printf("Error opening the db")
		return
	}
	defer db.Close()

	err = db.Put([]byte("apple"), []byte("valley"), nil)
	if err != nil {
		fmt.Printf("Could not write: %v\n", err)
		return
	}
	data, err := db.Get([]byte("apple"), nil)
	if err != nil {
		fmt.Printf("Could not read: %v\n", err)
		return
	}

	fmt.Printf("Here is the data: %v\n", string(data))
	err = db.Delete([]byte("apple"), nil)
	if err != nil {
		fmt.Printf("Could not delete: %v\n", err)
		return
	}

}
