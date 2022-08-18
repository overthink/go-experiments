package main

import (
	"sqlitefmt"
)

func main() {
	dbf, err := sqlitefmt.NewDbFile("test.db")
	defer dbf.Close()
	if err != nil {
		panic(err)
	}
	dbf.Header.Print()
}
