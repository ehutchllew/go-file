package main

import (
	"encoding/csv"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/xuri/excelize/v2"
)

var (
	aggFile *csv.Writer
	mu      sync.Mutex
	wg      sync.WaitGroup
)

func main() {
	var cwd, err = os.Getwd()
	if err != nil {
		log.Fatalf("Cannot Getwd\n Err: %v", err)
	}

	aggFile, err = setupWriteFile(fmt.Sprintf("%s/agg-file.csv", cwd))
	if err != nil {
		log.Fatalf("Failed to setup write file\n Err: %v", err)
	}

	assetsDirPath := fmt.Sprintf("%s/assets/Turin/Standard 8085915035 - 2022-01-30 Prior Month.xlsx", cwd)
	ch := make(chan []string, 20)
	// done := make(chan int8, 0)

	filepathErr := filepath.WalkDir(assetsDirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			wg.Add(1)
			parseExcelFile(path, &ch)
		}
		return nil
	})
	if filepathErr != nil {
		fmt.Printf("Error with filepath: %v", filepathErr)
	}

	// go readFromChannel(&ch, &done)
	wg.Wait()

	close(ch)
	// <-done
}

func parseExcelFile(filePath string, ch *chan []string) {
	defer wg.Done()

	file, err := excelize.OpenFile(filePath)
	if err != nil {
		log.Fatalf("Cannot open file (%s)\n Err: %v", filePath, err)
	}
	defer file.Close()

	filePathSlice := strings.Split(filepath.Dir(filePath), "/")
	devName := filePathSlice[len(filePathSlice)-1]
	fmt.Println("DEVNAME:::", devName)

	sheets := file.GetSheetList()
	for _, sheet := range sheets {
		rows, err := file.GetRows(sheet)
		if err != nil {
			log.Fatalf("Unable to get rows for sheet (%s)\n Err: %v", sheet, err)
		}

		for i, row := range rows {
			fmt.Printf("***\nRow%d Values: %v\n***\n", i, row)
		}

	}
}

func readFromChannel(ch *chan []string, done *chan int8) {
	for {
		data, ok := <-*ch
		if !ok {
			*done <- 1
			return
		}

		wg.Add(1)
		go writeToFile(&data)
		fmt.Println("CHANNEL DATA::: ", data)
	}
}

func setupWriteFile(path string) (*csv.Writer, error) {
	createdFile, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return csv.NewWriter(createdFile), nil
}

func writeToFile(data *[]string) {
	defer wg.Done()
	mu.Lock()
	aggFile.Write(*data)
	mu.Unlock()
}
