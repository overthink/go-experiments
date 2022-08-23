package sqlitefmt

import (
	"bytes"
	"encoding/binary"
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

type Page struct {
	Number uint32
	Data   []byte
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
	Payload      []byte // value, i.e. the row data
	OverflowPage uint32 // >0 iff payload overflows
}

func NewBTTableLeafCell(data []byte) (BTTableLeafCell, error) {
	println("---")
	println("cell data size", len(data))
	result := BTTableLeafCell{}
	r := bytes.NewReader(data)

	println("len1", r.Len())

	payloadSize, err := ReadVarint(r)
	if err != nil {
		return result, fmt.Errorf("error decoding payloadSize: %v", err)
	}
	result.PayloadSize = payloadSize

	println("len2", r.Len(), "payloadSize", payloadSize)

	rowid, err := ReadVarint(r)
	if err != nil {
		return result, fmt.Errorf("error decoding rowid: %v", err)
	}
	result.RowID = rowid

	println("len3", r.Len(), "rowid", rowid)

	payload := make([]byte, payloadSize)
	if err := binary.Read(r, binary.BigEndian, &payload); err != nil {
		return result, fmt.Errorf("error decoding payoad: %v", err)
	}
	result.Payload = payload

	return result, nil
}

type BTTableLeafPage struct {
	Header BTLeafPageHeader
	Cells  []BTTableLeafCell
}

// As defined in https://www.sqlite.org/fileformat.html#b_tree_pages
var maxVarintBytes = 9

// ReadVarint decodes a big-endian encoded varint from the reader r. We can't
// use binary.ReadVarint because it assumes the varint is stored little-endian.
// Based on https://cs.opensource.google/go/go/+/refs/tags/go1.19:src/encoding/binary/varint.go;l=129
func ReadVarint(r io.ByteReader) (int64, error) {
	var result int64
	shift := 0
	for i := 0; i < maxVarintBytes; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return result, nil
		}
		if b < 0b10000000 {
			// MSB not set, this is the last byte of the varint
			// TODO: overflow check?
			result = (result << shift) | int64(b)
			return result, nil
		}
		result = (result << shift) | int64(b&0b01111111)
		shift += 7
	}
	return result, nil
}

func NewBTTableLeafPage(page Page) (BTTableLeafPage, error) {
	result := BTTableLeafPage{}
	r := bytes.NewReader(page.Data)
	if page.Number == 1 {
		r.Seek(100, io.SeekStart)
	}
	if err := binary.Read(r, binary.BigEndian, &result.Header); err != nil {
		return result, fmt.Errorf("error decoding table leaf page header: %v", err)
	}

	cellOffsets := make([]uint16, result.Header.CellCount)
	if err := binary.Read(r, binary.BigEndian, &cellOffsets); err != nil {
		return result, fmt.Errorf("error decoding table leaf page cell offset: %v", err)
	}

	cells := make([]BTTableLeafCell, result.Header.CellCount)
	for i, offset := range cellOffsets {
		cell, err := NewBTTableLeafCell(page.Data[offset:])
		if err != nil {
			return result, fmt.Errorf("error decoding table leaf page cell: %v", err)
		}
		cells[i] = cell
	}
	result.Cells = cells
	return result, nil
}

/*
payload calc, table btree leaf page
pagesize = 4096
U = usable size = 4096-reserved space == 4096
X = max payload allowed on page = U-35 == 4061
P = payload (size of row in this case) = let's say it's big: 6000 bytes
M = min payload that must be on-page = ((U-12)*32/255)-23 == 485
K = ? "keep?" = M+((P-M)%(U-4)) == 485+((6000-485)%4092) == 1908

K is < X, so we'll store K==1908 bytes on the page.

Why this number? I think because it leaves 6000-1908==4092 bytes for the
overflow page. An overflow page format is 4-byte pointer to next page, then the
payload, so 4+4092==4096==pagesize so the overflow will be fully utilized. Clever.

It's different for btree index pages though. In those cases we want to make
sure a page can have at least 4 keys on it to ensure a reasonable fanout in the
tree. So the math is slightly different.

payload calc, index btree page
pagesize = 4096
U = usable size = 4096-reserved space == 4096
X = max payload allowed on page = ((U-12)*64/255)-23 == 1002
P = payload (size of index key) = let's say it's big: 6000 bytes
M = min payload that must be on-page = ((U-12)*32/255)-23 == 485
K = ? "keep?" = M+((P-M)%(U-4)) == 485+((6000-485)%4092) == 1908

K is > X, so we'll only keep M==485 bytes on the page, and the rest will go to
overflow.
*/

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

// Page returns the requested pageNum from the underlying db file.
func (dbf *DbFile) Page(pageNum uint32) (Page, error) {
	page := Page{}
	if pageNum > dbf.Header.NumPages {
		return page, fmt.Errorf(
			"asked for page %d but max page is %d",
			pageNum,
			dbf.Header.NumPages,
		)
	}
	page.Number = pageNum
	offset := int64(dbf.Header.PageSize) * int64(pageNum-1)
	_, err := dbf.file.Seek(offset, io.SeekStart)
	if err != nil {
		return page, err
	}
	page.Data = make([]byte, dbf.Header.PageSize)
	_, err = io.ReadAtLeast(dbf.file, page.Data, len(page.Data))
	return page, nil
}

func (dbf *DbFile) DecodeBTreePage(page Page) (interface{}, error) {
	r := bytes.NewReader(page.Data)
	if page.Number == 1 {
		r.Seek(100, io.SeekStart)
	}

	pageType, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("error reading pageType: %v", err)
	}

	if pageType == TableInterior || pageType == IndexInterior {
		result := BTInteriorPageHeader{}
		if err := binary.Read(r, binary.BigEndian, &result); err != nil {
			return nil, fmt.Errorf("failed to decode interior btree page header: %v", err)
		}
		return result, nil
	} else if pageType == TableLeaf || pageType == IndexLeaf {
		if pageType == TableLeaf {
			return NewBTTableLeafPage(page)
		}
		header := BTLeafPageHeader{}
		if err := binary.Read(r, binary.BigEndian, &header); err != nil {
			return nil, fmt.Errorf("failed to decode leaf btree page header: %v", err)
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
