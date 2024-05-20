package main

import (
	"fmt"
	"os"

	"github.com/zing22845/go-qpress"
)

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

	archiveFile := &qpress.ArchiveFile{}

	fmt.Println("filename: ", inputFile.Name())
	fmt.Println("filename: ", inputFile.Name())
	var limitSize int64 = 1024 * 1024
	isPartial, err := archiveFile.Decompress(inputFile, "./tmp/", limitSize)
	if err != nil {
		fmt.Printf("decompress qpress file failed: %s\n", err.Error())
		os.Exit(1)
	}
	if isPartial {
		fmt.Printf("partial decompress qpress file up to size: %d\n", limitSize)
	}
	/*
		err = archiveFile.DecompressStream(inputFile, os.Stdout, 1024*1024)
		if err != nil {
			fmt.Printf("decompress qpress file to stdout failed: %s\n", err.Error())
			os.Exit(1)
		}
	*/
}
