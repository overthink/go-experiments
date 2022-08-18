package main

import (
	"fmt"
	"sqlitefmt"
)

func main() {
	dbf, err := sqlitefmt.NewDbFile("test.db")
	defer dbf.Close()
	if err != nil {
		panic(err)
	}
	dbf.Header.Print()
	for i := uint32(1); i <= dbf.Header.NumPages; i++ {
		p, err := dbf.Page(i)
		if err != nil {
			panic(err)
		}
		fmt.Printf("page %d: %+v\n", i, p)
	}
}
