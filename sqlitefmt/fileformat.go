package sqlitefmt

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type DbFile struct {
	Filename string
	file     *os.File
	Header   DbHeader
}

func NewDbFile(filename string) (DbFile, error) {
	result := DbFile{}
	f, err := os.Open(filename)
	if err != nil {
		return result, fmt.Errorf("failed to open db: %v", err)
	}
	header, err := readHeader(f)
	if err != nil {
		return result, fmt.Errorf("failed to read db header: %v", err)
	}
	return DbFile{filename, f, header}, nil
}

func (dbf *DbFile) Close() error {
	if dbf.file != nil {
		return dbf.file.Close()
	}
	return nil
}

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

func (h *DbHeader) HeaderBytesString() string {
	// trim trailing \0
	return string(h.HeaderBytes[:15])

}
func (h *DbHeader) TextEncodingString() string {
	switch h.TextEncoding {
	case 1:
		return "UTF-8"
	case 2:
		return "UTF-16le"
	case 3:
		return "UTF-16be"
	}
	return "Unknown"
}

func (h *DbHeader) ReadVersionString() string {
	if h.ReadVersion == 1 {
		return "WAL"
	}
	return "legacy"
}

func (h *DbHeader) WriteVersionString() string {
	if h.WriteVersion == 1 {
		return "WAL"
	}
	return "legacy"
}

func (h *DbHeader) SQLiteVersionString() string {
	v := h.SQLiteVersion
	return fmt.Sprintf("%d.%d.%d", v/1000000, v/1000%1000, v%100)
}

func (h *DbHeader) Print() {
	println("Header string:      ", h.HeaderBytesString())
	println("Page size:          ", h.PageSize)
	println("Number of pages:    ", h.SizePages)
	println("File change count:  ", h.FileChangeCount)
	println("Text encoding:      ", h.TextEncodingString())
	println("Read version:       ", h.ReadVersionString())
	println("Write version:      ", h.WriteVersionString())
	println("First freelist page:", h.FirstFreelistTrunkPage)
	println("SQLite version:     ", h.SQLiteVersionString())
}

func readHeader(r io.Reader) (DbHeader, error) {
	result := DbHeader{}
	if err := binary.Read(r, binary.BigEndian, &result); err != nil {
		return DbHeader{}, fmt.Errorf("failed to decode header: %v", err)
	}
	return result, nil
}

type BTreeLeafPage struct {
	Type                uint8
	FirstFreeblock      uint16
	CellCount           uint16
	CellContentStart    uint16
	FragmentedFreeBytes uint8
}

type BTreeInteriorPage struct {
	BTreeLeafPage
	RightmostPointer uint32
}

type Page struct {
	data []byte
}

/*
func GetPage(f io.ReadSeeker, pageSize uint16, pageNum uint32) (Page, error) {
	offset := pageSize * (pageNum - 1)
	f.Seek(offset, io.SeekStart)
	return Page{}, nil
}
*/
