package sqlitefmt

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// BTree page type enum
const (
	IndexInterior byte = 0x02
	TableInterior      = 0x05
	IndexLeaf          = 0x0a
	TableLeaf          = 0x0d
)

type DbFile struct {
	Filename string
	file     *os.File
	Header   DbHeader
}

type BTLeafPageHeader struct {
	Type                byte
	FirstFreeblock      uint16
	CellCount           uint16
	CellContentStart    uint16
	FragmentedFreeBytes uint8
}

type BTInteriorPageHeader struct {
	BTLeafPageHeader
	RightmostPointer uint32
}

type varint = int64

type BTTableLeafCell struct {
	PayloadSize  varint
	RowID        varint // key
	Payload      []byte // value, I guess row data?
	OverflowPage uint32 // >0 if payload overflows
}

type BTLeafPage struct {
	Header       BTLeafPageHeader
	CellPointers []int16
	Cells        []BTTableLeafCell
	CellContent  []byte
}

func (p *BTLeafPage) HexDump() {
	println(hex.Dump(p.CellContent))
}

type OverflowPage struct {
	NextPage uint32
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

func (dbf *DbFile) Page(pageNum uint32) ([]byte, error) {
	if pageNum > dbf.Header.NumPages {
		return nil, fmt.Errorf(
			"asked for page %d but max page is %d",
			pageNum,
			dbf.Header.NumPages,
		)
	}
	offset := int64(dbf.Header.PageSize) * int64(pageNum-1)
	_, err := dbf.file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	data := make([]byte, dbf.Header.PageSize)
	_, err = io.ReadAtLeast(dbf.file, data, int(dbf.Header.PageSize))
	if pageNum == 1 {
		// Page 1 contains 100 bytes of db header, so chuck that
		data = data[100:]
	}
	return data, nil
}

func (dbf *DbFile) DecodeBTreePage(pageData []byte) (interface{}, error) {
	pageType := pageData[0]
	r := bytes.NewReader(pageData)
	if pageType == TableInterior || pageType == IndexInterior {
		result := BTInteriorPageHeader{}
		if err := binary.Read(r, binary.BigEndian, &result); err != nil {
			return nil, fmt.Errorf("failed to decode interior btree page header: %v", err)
		}
		return result, nil
	} else if pageType == TableLeaf || pageType == IndexLeaf {
		header := BTLeafPageHeader{}
		if err := binary.Read(r, binary.BigEndian, &header); err != nil {
			return nil, fmt.Errorf("failed to decode leaf btree page header: %v", err)
		}
		if pageType == TableLeaf {
			page := BTLeafPage{
				Header:      header,
				CellContent: pageData[header.CellContentStart:],
			}
			return page, nil
		}
		return header, nil
	}
	// TODO: handle non-btree pages like payload overflow, and ptrmap
	return nil, fmt.Errorf("invalid btree page type: %v", pageType)
}

// DbHeader is the format of the first 100 bytes of an SQLite file.
// https://www.sqlite.org/fileformat.html#the_database_header
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
	NumPages                   uint32
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
	println("Reserved space:     ", h.ReservedSpace)
	println("Number of pages:    ", h.NumPages)
	println("File change count:  ", h.FileChangeCount)
	println("1st freelist page:  ", h.FirstFreelistTrunkPage)
	println("Freelist page count:", h.FreelistPagesCount)
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
