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
	err = archiveFile.Decompress(inputFile, "./tmp/", 1024*1024)
	if err != nil {
		fmt.Printf("decompress qpress file failed: %s\n", err.Error())
		os.Exit(1)
	}
	/*
		err = archiveFile.DecompressStream(inputFile, os.Stdout, 1024*1024)
		if err != nil {
			fmt.Printf("decompress qpress file to stdout failed: %s\n", err.Error())
			os.Exit(1)
		}
	*/
}
