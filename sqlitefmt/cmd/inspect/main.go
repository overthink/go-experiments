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
		pageData, err := dbf.Page(i)
		if err != nil {
			panic(err)
		}
		// TODO: false assumption here that all pages are btree pages
		p, err := dbf.DecodeBTreePage(pageData)
		if err != nil {
			panic(err)
		}
		fmt.Printf("page %d:\n", i)
		if leaf, ok := p.(sqlitefmt.BTLeafPage); ok {
			fmt.Printf("type: %v\n", leaf.Header.Type)
			leaf.HexDump()
		} else {
			fmt.Printf("%+v\n\n", p)
		}
		if i > 5 {
			break
		}
	}
}
