package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

type DbHeader struct {
	HeaderBytes                [16]byte
	PageSize                   uint16
	WriteVersion               uint8
	ReadVersion                uint8
	ReservedSpace              uint8
	MaxEmbeddedPayloadFraction uint8
	MinEmbeddedPayloadFraction uint8
	LeafPayloadFraction        uint8
	FileChangeCount            uint32
	SizePages                  uint32
	FirstFreelistTrunkPage     uint32
	FreelistPagesCount         uint32
	SchemaCookie               uint32
	SchemaFormat               uint32
	DefaltPageCacheSize        uint32
	LargestRootBTreePage       uint32
	TextEncoding               uint32
	UserVersion                uint32
	IncrementalVacuumMode      uint32
	ApplicationID              uint32
	Reserved                   [20]byte
	VersionValidFor            uint32
	SQLiteVersion              uint32
}

func readHeader(r io.Reader) (DbHeader, error) {
	result := DbHeader{}
	if err := binary.Read(r, binary.BigEndian, &result); err != nil {
		return DbHeader{}, fmt.Errorf("failed to decode header: %v", err)
	}
	return result, nil
}

func main() {
	f, err := os.Open("test.db")
	if err != nil {
		log.Fatal("failed to open db", err)
	}
	defer f.Close()
	header, err := readHeader(f)
	if err != nil {
		log.Fatalf("failed to read sqllite header: %v", err)
	}
	fmt.Printf("header: %v\n", header)
}
