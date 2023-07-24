package main

import (
	"bufio"
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

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Cannot open file (%s)\n Err: %v", filePath, err)
	}
	defer file.Close()

	filePathSlice := strings.Split(filepath.Dir(filePath), "/")
	devName := filePathSlice[len(filePathSlice)-1]
	fmt.Println("DEVNAME:::", devName)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		reader := csv.NewReader(strings.NewReader(line))

		csvValues, err := reader.Read()
		if err != nil {
			fmt.Printf("Error reading CSV line (%v)\n Err: %v", line, err)
			continue
		}

		// Depending on `csvValues` type, we might be able to prepend `devName` before writing to channel
		fmt.Println("CSV Values:", csvValues)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Scanner error:", err)
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
