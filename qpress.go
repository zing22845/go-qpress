package main

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

	quicklz "github.com/Hiroko103/go-quicklz"

	"github.com/alitto/pond"
)

const (
	TypeDown         = 'D'
	TypeUp           = 'U'
	TypeFile         = 'F'
	TypeNew          = 'N'
	TypeEnd          = 'E'
	DefaultChunkSize = 65536
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
	DecompressedChunk  []byte
	DecompressedSize   int64
	DecompressedOffset int64
}

type FileTrailer struct {
	TrailerType TargetType
	StarterTail
	RecoverInfo
}

func (ri *RecoverInfo) ReadRecoverInfo(r io.Reader) (err error) {
	n, err := r.Read(ri[:])
	if err != nil {
		return err
	}
	if n != 8 {
		return io.ErrUnexpectedEOF
	}
	/*
		if !bytes.Equal(ri[:], EmptyRecoverInfo) {
			return fmt.Errorf("non-empty recover info: %v, %v", ri[:], EmptyRecoverInfo)
		}
	*/
	return
}

func (s *StarterTail) ReadStarterTail(r io.Reader) (err error) {
	n, err := r.Read(s[:])
	if err != nil {
		return err
	}
	if n != 7 {
		return io.ErrUnexpectedEOF
	}
	return
}

func (t *TargetType) ReadType(r io.Reader) (err error) {
	*t, err = ReadByte(r)
	return err
}

func (af *ArchiveFile) Decompress(r io.Reader) (err error) {
	// defer profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook).Stop()
	err = af.ReadFileHeader(r)
	if err != nil {
		return fmt.Errorf("read file header failed: %s", err.Error())
	}

	tt := new(TargetType)
	for {
		err = tt.ReadType(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read type %s failed: %s", tt[:], err.Error())
		}
		switch tt[0] {
		case 0:
			return
		case TypeDown:
			DownTarget := new(DownTarget)
			DownTarget.TargetType = *tt
			err = DownTarget.Read(r)
			if err != nil {
				return err
			}
			af.Targets = append(af.Targets, DownTarget)
			return fmt.Errorf("unsupport down directory")
		case TypeUp:
			UpTarget := new(UpTarget)
			UpTarget.TargetType = *tt
			err = UpTarget.Read(r)
			if err != nil {
				return err
			}
			af.Targets = append(af.Targets, UpTarget)
			return fmt.Errorf("unsupport up directory")
		case TypeFile:
			FileTarget := &FileTarget{}
			FileTarget.TargetType = *tt
			err = FileTarget.Decompress(r)
			if err != nil {
				return err
			}
			af.Targets = append(af.Targets, FileTarget)
		default:
			return fmt.Errorf("unknown type: %s", tt[:])
		}
	}
}

func (ah *ArchiveHeader) ReadFileHeader(r io.Reader) (err error) {
	// get qpress magic
	_, err = r.Read(ah.Magic[:])
	if err != nil {
		return fmt.Errorf("read magic failed: %s", err.Error())
	}
	// check qpress magic
	if !bytes.Equal(ah.Magic[:], QpressMagic) {
		return fmt.Errorf("invalid magic: %s", ah.Magic)
	}

	// get chunk size
	chunkSizeBytes := make([]byte, 8)
	_, err = r.Read(chunkSizeBytes)
	if err != nil {
		return fmt.Errorf("read chunk size failed: %s", err.Error())
	}
	ah.ChunkSize = binary.LittleEndian.Uint64(chunkSizeBytes)
	ChunkSize = ah.ChunkSize
	return
}

func (t *TargetHeader) ReadHeader(r io.Reader) (err error) {
	t.NameLength, t.Name, err = ReadLength32EncodedString(r)
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

func (t *FileTarget) Decompress(r io.Reader) (err error) {
	var offset int64

	err = t.ReadHeader(r)
	if err != nil {
		return err
	}

	// create decompressed file
	f, err := os.OpenFile(string(t.Name), os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	defer f.Close()

	var maxWorkers = 8
	var maxDataBlockQueue = 10000

	pool := pond.New(maxWorkers, maxDataBlockQueue)

	defer pool.StopAndWait()

	// decompress blocks
	tt := new(TargetType)
	for {
		if err != nil {
			return err
		}
		err := tt.ReadType(r)
		if err != nil {
			return fmt.Errorf("read type %s failed: %s", tt[:], err.Error())
		}
		switch tt[0] {
		case TypeNew:
			block := &DataBlock{}
			err = block.ReadBlock(r)
			if err != nil {
				return fmt.Errorf("decompress block failed: %s", err.Error())
			}
			block.DecompressedOffset = offset
			pool.Submit(func() {
				err := block.DecompressChunk()
				if err != nil {
					fmt.Printf("decompress chunk failed: %s", err.Error())
					return
				}
				_, err = f.WriteAt(block.DecompressedChunk, block.DecompressedOffset)
				if err != nil {
					fmt.Printf("write failed: %s\n", err.Error())
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
	n, err := r.Read(t.Checksum[:])
	if err != nil {
		return err
	}
	if n != 4 {
		return fmt.Errorf("invalid checksum: %s", t.Checksum)
	}
	return
}

func (t *DataBlock) ReadChunk(r io.Reader) (err error) {
	t.CompressedChunk = make([]byte, ChunkSize+400)

	// read header of CompressedChunk
	header := t.CompressedChunk[:9]
	n, err := r.Read(header)
	if err != nil {
		return err
	}
	if n != 9 {
		return fmt.Errorf("read chunk header failed")
	}

	// read CompressedChunk
	t.CompressedSize = quicklz.Size_compressed(&header)
	n, err = r.Read(t.CompressedChunk[9:t.CompressedSize])
	if err != nil {
		return err
	}
	if int64(n) != t.CompressedSize-9 {
		return fmt.Errorf("read chunk size %d is not equal expect %d", n, t.CompressedSize-9)
	}

	// init DecompressedChunk
	t.DecompressedSize = quicklz.Size_decompressed(&header)
	t.DecompressedChunk = make([]byte, t.DecompressedSize)
	return
}

func (t *DataBlock) DecompressChunk() (err error) {
	qlz, err := quicklz.New(quicklz.COMPRESSION_LEVEL_1, quicklz.STREAMING_BUFFER_100000)
	if err != nil {
		return err
	}

	// Decompress data to DecompressedChunk
	part := t.CompressedChunk[:t.CompressedSize]
	n, err := qlz.Decompress(&part, &t.DecompressedChunk)
	if err != nil {
		return fmt.Errorf("decompress: %s", err.Error())
	}
	t.DecompressedChunk = t.DecompressedChunk[:n]

	return
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
	_, err = io.ReadFull(r, b[:])
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

func ReadLength32EncodedString(r io.Reader) (readBytesLen uint32, readBytes []byte, err error) {
	// get length
	lenBuf := make([]byte, 4)
	n, err := r.Read(lenBuf)
	if err != nil {
		return readBytesLen, readBytes, err
	}
	if n != 4 {
		return readBytesLen, readBytes, io.ErrUnexpectedEOF
	}

	// get string
	readBytesLen = binary.LittleEndian.Uint32(lenBuf)
	readBytes = make([]byte, readBytesLen)
	n, err = r.Read(readBytes)
	if err != nil {
		return readBytesLen, readBytes, err
	}
	if uint32(n) != readBytesLen {
		return readBytesLen, readBytes, io.ErrUnexpectedEOF
	}

	return readBytesLen, readBytes, err
}

func main() {
	var err error
	var inputFile *os.File
	var inputFileName string

	if len(os.Args) > 1 {
		inputFileName = os.Args[1]
	} else {
		inputFileName = "mysql.ibd.qp"
	}

	if inputFileName != "" {
		inputFile, err = os.Open(inputFileName)
		if err != nil {
			fmt.Printf("open file failed: %s\n", err.Error())
			os.Exit(1)
		}
		defer inputFile.Close()
	}

	archiveFile := &ArchiveFile{}

	fmt.Println("filename: ", inputFile.Name())
	err = archiveFile.Decompress(inputFile)
	if err != nil {
		fmt.Printf("decompress qpress file failed: %s\n", err.Error())
		os.Exit(1)
	}
}
