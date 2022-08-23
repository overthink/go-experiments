package main

import (
	"fmt"
	"os"
	"sqlitefmt"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("usage: %s DBFILE\n", os.Args[0])
		os.Exit(1)
	}
	dbf, err := sqlitefmt.NewDbFile(os.Args[1])
	defer dbf.Close()
	if err != nil {
		panic(err)
	}
	dbf.Header.Print()
	for i := uint32(1); i <= dbf.Header.NumPages; i++ {
		page, err := dbf.Page(i)
		if err != nil {
			panic(err)
		}
		// TODO: false assumption here that all pages are btree pages
		decoded, err := dbf.DecodeBTreePage(page)
		if err != nil {
			panic(err)
		}
		fmt.Printf("page %d:\n", i)
		if leaf, ok := decoded.(sqlitefmt.BTTableLeafPage); ok {
			fmt.Printf("type: %v\n", leaf.Header.Type)
			fmt.Printf("%+v\n", leaf)
		} else {
			fmt.Printf("%+v\n\n", decoded)
		}
		if i > 5 {
			break
		}
	}
}
