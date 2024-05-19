package qpress

/*
This is a go version implementation of qpress(github.com/PierreLvx/qpress)

qpress - portable high-speed file archiver
Copyright Lasse Reinhold 2006-2010
GPL 1, 2 and 3 licensed.

An archive file consists of "D" and "U" characters which instruct the decompressor to traverse up and down in
directories to create a directory three. The "F" character instructs it to create a file:

ARCHIVE =        ARCHIVEHEADER + (1 or more of UPDIR | DOWNDIR | FILE)
ARCHIVEHEADER =  "qpress10" + (ui64)(chunk size of decompressed packets)
DOWNDIR =        "D" + (ui32)(size of directory name) + (directory name) + (char)0
UPDIR =          "U"
FILE =           FILEHEADER + (0 or more of DATABLOCK) + FILETRAILER
FILEHEADER =     "F" + (ui32)(size of file name) + (file name) + (char)0
DATABLOCK =      "NEWBNEWB" + (ui64)(recovery information) + (ui32)(adler32 of compressed block) + (compressed packet)
FILETRAILER =    "ENDSENDS" + (ui64)(recovery information)

The values (ui32) and (ui64) are stored in little endian format. Example of compressing following directory structure:

FOO             directory
	c.txt       file containing "hello"
	BAR         empty directory
d.txt           file containing "there"

Two digit values are in hexadecimal, remaining values are printable and represented by their character:

          0   1   2   3   4   5   6   7   8   9   a   b   c   d   e   f
000000    q   p   r   e   s   s   1   0  00  00  01  00  00  00  00  00
000010    F  05  00  00  00   c   .   t   x   t  00   N   E   W   B   N
000020    E   W   B  00  00  00  00  00  00  00  00  eb  02   %  0d   E
000030   0c  05  00  00  00  80   h   e   l   l   o   E   N   D   S   E
000040    N   D   S  00  00  00  00  00  00  00  00   D  03  00  00  00
000050    F   O   O  00   D  03  00  00  00   B   A   R  00   F  05  00
000060   00  00   d   .   t   x   t  00   N   E   W   B   N   E   W   B
000070   00  00  00  00  00  00  00  00  ef  02   Z  0d   E  0c  05  00
000080   00  00  80   t   h   e   r   e   E   N   D   S   E   N   D   S
000090   00  00  00  00  00  00  00  00   U   U

Offsets 2f - 3a and 7c - 87 are compressed packets. You see "hello" and "there" in plaintext because input is too small
to compress.
*/

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	quicklz "github.com/Hiroko103/go-quicklz"

	"github.com/alitto/pond"
)

const (
	TypeDown         = 'D'
	TypeUp           = 'U'
	TypeFile         = 'F'
	TypeNew          = 'N'
	TypeEnd          = 'E'
	DefaultChunkSize = 1024
	ChunkHeaderSize  = 9
)

var (
	QpressMagic             = []byte{'q', 'p', 'r', 'e', 's', 's', '1', '0'}
	BlockStarter            = []byte{TypeNew, 'E', 'W', 'B', 'N', 'E', 'W', 'B'}
	TrailerStarter          = []byte{TypeEnd, 'N', 'D', 'S', 'E', 'N', 'D', 'S'}
	EmptyRecoverInfo        = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	Terminator              = []byte{0}
	ChunkSize        uint64 = DefaultChunkSize
)

// RecoverInfo in both DATABLOCK and FILETRAILER
type RecoverInfo [8]byte

// StarterTail in both DATABLOCK and FILETRAILER
type StarterTail [7]byte

// TargetType in DOWNDIR, UPDIR, FILEHEADER, DATABLOCK and FILETRAILER
type TargetType [1]byte

type Target interface {
	ReadType(r io.Reader) (err error)
	ReadHeader(r io.Reader) (err error)
}

// Reader provides sequential access to chunks from an qpress. Each chunk returned represents a
// contiguous set of bytes for a file compressed in the qpress file. The Next method advances the stream
// and returns the next chunk in the archive. Each archive then acts as a reader for its contiguous set of bytes
type Reader struct {
	reader io.Reader
}

// NewReader creates a new Reader by wrapping the provided reader
func NewReader(reader io.Reader) *Reader {
	return &Reader{reader: reader}
}

// Next advances the Reader and returns the next DataBlock.
func (r *Reader) NextBlock() (dataBlock *DataBlock, err error) {
	return
}

// ARCHIVE =        ARCHIVEHEADER + (1 or more of UPDIR | DOWNDIR | FILE)
type ArchiveFile struct {
	ArchiveHeader
	Targets []Target
}

// ARCHIVEHEADER =  "qpress10" + (ui64)(chunk size of decompressed packets)
type ArchiveHeader struct {
	Magic     [8]byte
	ChunkSize uint64
}

type TargetHeader struct {
	TargetType
	NameLength uint32
	Name       []byte
	Terminator [1]byte
}

type UpTarget struct {
	TargetHeader
}

type DownTarget struct {
	TargetHeader
}

type FileTarget struct {
	TargetHeader
	DataBlocks []*DataBlock
	FileTrailer
}

type DataBlock struct {
	BlockType TargetType
	StarterTail
	RecoverInfo
	Checksum           [4]byte
	CompressedChunk    []byte
	CompressedSize     int64
	DecompressedSize   int64
	DecompressedOffset int64
}

type FileTrailer struct {
	TrailerType TargetType
	StarterTail
	RecoverInfo
}

// ReadRecoverInfo reads the RecoverInfo from the given reader. It returns
// an error if the reader returns an error, or if the reader returns fewer
// than 8 bytes.
func (ri *RecoverInfo) ReadRecoverInfo(r io.Reader) (err error) {
	err = binary.Read(r, nil, ri)
	return
}

// ReadStarterTail reads the starter tail from the given io.Reader.
//
// The starter tail is a sequence of bytes that identifies the start of
// a starter file. It is typically seven bytes long.
//
// The starter tail consists of the characters 'S' 'T' 'A' 'R' 'T' 'E' 'R'
// and is followed by a newline character.
//
// If the given io.Reader returns an error before the entire starter tail
// is read, ReadStarterTail returns the number of bytes read and the error.
// If the given io.Reader returns io.EOF before the entire starter tail is
// read, ReadStarterTail returns io.ErrUnexpectedEOF.
func (s *StarterTail) ReadStarterTail(r io.Reader) (err error) {
	return binary.Read(r, nil, s)
}

// ReadType reads a TargetType from the given Reader.
func (t *TargetType) ReadType(r io.Reader) (err error) {
	*t, err = ReadByte(r)
	return
}

// Decompress reads the archive file header and then processes each target
// until it finds the end of the file.
func (af *ArchiveFile) Decompress(r io.Reader, baseDIR string, limitSize int64) (isPartial bool, err error) {
	// Read the archive file header.
	err = af.ReadFileHeader(r)
	if err != nil {
		return false, fmt.Errorf("read file header failed: %s", err.Error())
	}
	err = os.Mkdir("tmp", 0755)
	if err != nil && !os.IsExist(err) {
		return false, err
	}

	tt := new(TargetType)
	for {
		// Read the target type.
		err = tt.ReadType(r)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("read type %s failed: %s", tt[:], err.Error())
		}

		// Process the target based on its type.
		switch tt[0] {
		case 0:
			return false, nil
		case TypeDown:
			DownTarget := new(DownTarget)
			DownTarget.TargetType = *tt
			err = DownTarget.Read(r)
			if err != nil {
				return false, err
			}
			af.Targets = append(af.Targets, DownTarget)
			return false, fmt.Errorf("unsupport down directory")
		case TypeUp:
			UpTarget := new(UpTarget)
			UpTarget.TargetType = *tt
			err = UpTarget.Read(r)
			if err != nil {
				return false, err
			}
			af.Targets = append(af.Targets, UpTarget)
			return false, fmt.Errorf("unsupport up directory")
		case TypeFile:
			FileTarget := &FileTarget{}
			FileTarget.TargetType = *tt
			err = FileTarget.Decompress(r, baseDIR, limitSize)
			if err != nil {
				if strings.HasPrefix(err.Error(), "partial decompress to limited size") {
					return true, nil
				}
				return false, err
			}
			af.Targets = append(af.Targets, FileTarget)
		default:
			return false, fmt.Errorf("unknown type: %s", tt[:])
		}
	}
}

func (af *ArchiveFile) DecompressStream(r io.Reader, w io.Writer, limitSize int64) (isPartial bool, err error) {
	// Read the archive file header.
	err = af.ReadFileHeader(r)
	if err != nil {
		return false, fmt.Errorf("read file header failed: %s", err.Error())
	}
	err = os.Mkdir("tmp", 0755)
	if err != nil && !os.IsExist(err) {
		return false, err
	}

	tt := new(TargetType)
	for {
		// Read the target type.
		err = tt.ReadType(r)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("read type %s failed: %s", tt[:], err.Error())
		}

		// Process the target based on its type.
		switch tt[0] {
		case 0:
			return false, nil
		case TypeDown:
			DownTarget := new(DownTarget)
			DownTarget.TargetType = *tt
			err = DownTarget.Read(r)
			if err != nil {
				return false, err
			}
			af.Targets = append(af.Targets, DownTarget)
			return false, fmt.Errorf("unsupport down directory")
		case TypeUp:
			UpTarget := new(UpTarget)
			UpTarget.TargetType = *tt
			err = UpTarget.Read(r)
			if err != nil {
				return false, err
			}
			af.Targets = append(af.Targets, UpTarget)
			return false, fmt.Errorf("unsupport up directory")
		case TypeFile:
			FileTarget := &FileTarget{}
			FileTarget.TargetType = *tt
			err = FileTarget.DecompressStream(r, w, limitSize)
			if err != nil {
				if strings.HasPrefix(err.Error(), "partial decompress to limited size") {
					return true, nil
				}
				return false, err
			}
			af.Targets = append(af.Targets, FileTarget)
		default:
			return false, fmt.Errorf("unknown type: %s", tt[:])
		}
	}
}

// ReadFileHeader reads and verifies the magic number and chunk size from the
// archive header. It also sets the ChunkSize variable to the chunk size read
// from the archive header.
func (ah *ArchiveHeader) ReadFileHeader(r io.Reader) (err error) {
	// get qpress magic
	err = binary.Read(r, nil, &ah.Magic)
	if err != nil {
		return fmt.Errorf("read magic failed: %s", err.Error())
	}
	// check qpress magic
	if !bytes.Equal(ah.Magic[:], QpressMagic) {
		return fmt.Errorf("invalid magic: %s", ah.Magic)
	}
	// get chunk size
	err = binary.Read(r, binary.LittleEndian, &ah.ChunkSize)
	if err != nil {
		return fmt.Errorf("read chunk size failed: %s", err.Error())
	}
	ChunkSize = ah.ChunkSize
	return
}

func (t *TargetHeader) ReadHeader(r io.Reader) (err error) {
	t.NameLength, t.Name, err = ReadLengthU32EncodedString(r)
	if err != nil {
		return err
	}
	t.Terminator, err = ReadTerminator(r)
	return err
}

func (t *UpTarget) Read(r io.Reader) (err error) {
	return t.ReadHeader(r)
}

func (t *UpTarget) ReadHeader(r io.Reader) (err error) {
	return nil
}

func (t *DownTarget) Read(r io.Reader) (err error) {
	return t.ReadHeader(r)
}

func (t *FileTarget) Decompress(r io.Reader, baseDIR string, limitSize int64) (err error) {
	var offset int64

	err = t.ReadHeader(r)
	if err != nil {
		return err
	}

	targetFilePath := filepath.Join(baseDIR, string(t.Name))
	_, err = os.Stat(targetFilePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	} else if err == nil {
		return fmt.Errorf("file %s already exists", targetFilePath)
	}
	// create decompressed file
	f, err := os.OpenFile(targetFilePath, os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	defer f.Close()

	var maxWorkers = 10
	var maxDataBlockQueue = 40

	pool := pond.New(maxWorkers, maxDataBlockQueue, pond.Strategy(pond.Balanced()))

	defer pool.StopAndWait()

	// decompress blocks
	tt := new(TargetType)
	for {
		if err != nil {
			return err
		}
		err := tt.ReadType(r)
		if err != nil {
			return fmt.Errorf("read type %s failed: %w", tt[:], err)
		}
		switch tt[0] {
		case TypeNew:
			block := NewDataBlock()
			err = block.ReadBlock(r)
			if err != nil {
				return fmt.Errorf("decompress block failed: %w", err)
			}
			if limitSize > 0 && offset+block.DecompressedSize > limitSize {
				return fmt.Errorf("partial decompress to limited size %d", limitSize)
			}
			block.DecompressedOffset = offset
			pool.Submit(func() {
				decompressedChunk := make([]byte, block.DecompressedSize)
				err := block.DecompressChunk(&decompressedChunk)
				if err != nil {
					fmt.Printf("decompress chunk failed: %+v", err)
					return
				}
				_, err = f.WriteAt(decompressedChunk, block.DecompressedOffset)
				if err != nil {
					fmt.Printf("write failed: %+v", err)
					return
				}
			})
			offset += block.DecompressedSize
		case TypeEnd:
			err = t.ReadTrailer(r)
			if err != nil {
				return fmt.Errorf("read trailer failed: %s", err.Error())
			}
			return nil
		default:
			return fmt.Errorf("invalid block header, 'N' or 'E' not found, get: %d", tt[:])
		}
	}
}

func (t *FileTarget) DecompressStream(r io.Reader, w io.Writer, limitSize int64) (err error) {
	var offset int64

	err = t.ReadHeader(r)
	if err != nil {
		return err
	}

	// decompress blocks
	tt := new(TargetType)
	for {
		if err != nil {
			return err
		}
		err := tt.ReadType(r)
		if err != nil {
			return fmt.Errorf("read type %s failed: %w", tt[:], err)
		}
		switch tt[0] {
		case TypeNew:
			block := NewDataBlock()
			err = block.ReadBlock(r)
			if err != nil {
				return fmt.Errorf("decompress block failed: %w", err)
			}
			if limitSize > 0 && offset+block.DecompressedSize > limitSize {
				return fmt.Errorf("partial decompress to limited size %d", limitSize)
			}
			block.DecompressedOffset = offset
			decompressedChunk := make([]byte, block.DecompressedSize)
			err = block.DecompressChunk(&decompressedChunk)
			if err != nil {
				return fmt.Errorf("decompress chunk failed: %+v", err)
			}
			_, err = w.Write(decompressedChunk)
			if err != nil {
				return fmt.Errorf("write failed: %+v", err)
			}
			offset += block.DecompressedSize
		case TypeEnd:
			err = t.ReadTrailer(r)
			if err != nil {
				return fmt.Errorf("read trailer failed: %s", err.Error())
			}
			return nil
		default:
			return fmt.Errorf("invalid block header, 'N' or 'E' not found, get: %d", tt[:])
		}
	}
}

func NewDataBlock() *DataBlock {
	return &DataBlock{
		BlockType:          TargetType{TypeNew},
		CompressedChunk:    make([]byte, ChunkSize, ChunkSize+400),
		CompressedSize:     0,
		DecompressedSize:   0,
		DecompressedOffset: 0,
	}
}

func (t *DataBlock) InitBlock() error {
	t.CompressedChunk = make([]byte, ChunkSize, ChunkSize+400)
	t.CompressedSize = 0
	t.DecompressedSize = 0
	t.DecompressedOffset = 0
	return nil
}

func (t *DataBlock) ReadBlock(r io.Reader) (err error) {
	err = t.ReadStarterTail(r)
	if err != nil {
		return err
	}
	if !bytes.Equal(t.StarterTail[:], BlockStarter[1:]) {
		return fmt.Errorf("invalid block starter tail: %s", t.StarterTail)
	}
	err = t.ReadRecoverInfo(r)
	if err != nil {
		return err
	}
	err = t.ReadChecksum(r)
	if err != nil {
		return err
	}
	err = t.ReadChunk(r)
	if err != nil {
		return err
	}
	return
}

func (t *DataBlock) ReadChecksum(r io.Reader) (err error) {
	err = binary.Read(r, nil, &t.Checksum)
	return
}

func (t *DataBlock) ReadChunk(r io.Reader) (err error) {
	// read header of CompressedChunk
	header := t.CompressedChunk[:ChunkHeaderSize]
	err = binary.Read(r, nil, &header)
	if err != nil {
		return err
	}
	// read CompressedChunk
	t.CompressedSize = quicklz.Size_compressed(&header)
	payload := t.CompressedChunk[ChunkHeaderSize:t.CompressedSize]
	err = binary.Read(r, nil, payload)
	if err != nil {
		return err
	}
	// init DecompressedChunk
	t.DecompressedSize = quicklz.Size_decompressed(&header)
	return
}

func (t *DataBlock) DecompressChunk(decompressedChunk *[]byte) (err error) {
	qlz, err := quicklz.New(quicklz.COMPRESSION_LEVEL_1, quicklz.STREAMING_BUFFER_0)
	if err != nil {
		return err
	}
	// Decompress data to DecompressedChunk
	_, err = qlz.Decompress(&t.CompressedChunk, decompressedChunk)
	if err != nil {
		return fmt.Errorf("decompress: %s", err.Error())
	}
	return nil
}

func (t *FileTrailer) ReadTrailer(r io.Reader) (err error) {
	err = t.ReadStarterTail(r)
	if err != nil {
		return err
	}
	if !bytes.Equal(t.StarterTail[:], TrailerStarter[1:]) {
		return fmt.Errorf("invalid trailer starter tail: %s", t.StarterTail)
	}

	// read recover info
	return t.ReadRecoverInfo(r)
}

func ReadByte(r io.Reader) (b [1]byte, err error) {
	err = binary.Read(r, nil, &b)
	return b, err
}

func ReadTerminator(r io.Reader) (terminator [1]byte, err error) {
	terminator, err = ReadByte(r)
	if err != nil {
		return terminator, fmt.Errorf("read terminator failed: %s", err.Error())
	}
	if terminator[0] != Terminator[0] {
		return terminator, fmt.Errorf("invalid terminator: %s", terminator)
	}
	return terminator, err
}

func ReadLengthU32EncodedString(r io.Reader) (readBytesLen uint32, readBytes []byte, err error) {
	// get length
	err = binary.Read(r, binary.LittleEndian, &readBytesLen)
	if err != nil {
		return readBytesLen, readBytes, err
	}

	// get bytes
	readBytes = make([]byte, readBytesLen)
	err = binary.Read(r, nil, &readBytes)
	return readBytesLen, readBytes, err
}
