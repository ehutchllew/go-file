package cmd

import (
	"encoding/csv"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/xuri/excelize/v2"
)

var (
	aggFile        *csv.Writer
	containsHeader bool
	mu             sync.Mutex
	wg             sync.WaitGroup
	validationFile *csv.Writer
)

var (
	rootCmd = &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			var cwd, err = os.Getwd()
			if err != nil {
				log.Fatalf("Cannot Getwd\n Err: %v", err)
			}

			devName := cmd.Flag("file").Value.String()

			aggFile, err = setupWriteFile(
				fmt.Sprintf(
					"%s/%s - all bcrs - %v.csv",
					cwd,
					devName,
					time.Now().UTC(),
				),
			)
			if err != nil {
				log.Fatalf("Failed to setup agg write file\n Err: %v", err)
			}
			defer aggFile.Flush()

			validationFile, err = setupWriteFile(
				fmt.Sprintf(
					"%s/%s - missing fields - %v.csv",
					cwd,
					devName,
					time.Now().UTC(),
				),
			)
			if err != nil {
				log.Fatalf("Failed to setup validation write file\n Err: %v", err)
			}
			defer validationFile.Flush()

			assetsDirPath := fmt.Sprintf("%s/assets/%s", cwd, devName)

			ch := make(chan []string, 100)
			done := make(chan int8, 0)

			filepathErr := filepath.WalkDir(assetsDirPath, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}

				if !d.IsDir() && filepath.Ext(path) == ".xlsx" {
					fmt.Println("wg add parseExcelFile")
					wg.Add(1)
					go parseExcelFile(path, &ch)
				}
				return nil
			})
			if filepathErr != nil {
				fmt.Printf("Error with filepath: %v", filepathErr)
			}

			fmt.Println("\nREADING CHAN~~~")
			go readFromChannel(&ch, &done)
			fmt.Println("\nWAITING~~~")
			wg.Wait()

			fmt.Println("\nCLOSING CHAN~~~")
			close(ch)
			fmt.Println("\nWAITING DONE CHAN~~~")
			<-done
			fmt.Println("\nDONE CHAN~~~")
		},
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringP("file", "f", "", "provide path to file or directory to parse")

	rootCmd.MarkFlagRequired("file")
}

func parseExcelFile(filePath string, ch *chan []string) {
	defer func() {
		fmt.Println("defer parseExcelFile")
		wg.Done()
	}()

	file, err := excelize.OpenFile(filePath)
	if err != nil {
		log.Fatalf("Cannot open file (%s)\n Err: %v", filePath, err)
	}
	defer file.Close()

	sheets := file.GetSheetList()
	for _, sheet := range sheets {
		rows, err := file.GetRows(sheet)
		if err != nil {
			log.Fatalf("Unable to get rows for sheet (%s)\n Err: %v", sheet, err)
		}

		for i, row := range rows {
			// TODO: sanitize row
			sanitizeRow(row)
			if i == 0 {
				validateFile(filepath.Base(filePath), row)
				fmt.Println("wg add writeToAggFile")
				wg.Add(1)
				writeToAggFile(&row)
			} else {
				fmt.Println("wg add writeToAggFile")
				wg.Add(1)
				*ch <- row
			}
		}

	}
}

func readFromChannel(ch *chan []string, done *chan int8) {
	defer func() {
		fmt.Println("defer readFromChannel")
	}()
	for {
		data, ok := <-*ch
		if !ok {
			fmt.Println("END OF READ")
			*done <- 1
			return
		}

		go writeToAggFile(&data)
		// fmt.Println("CHANNEL DATA::: ", data)
	}
}

func sanitizeRow(row []string) []string {
	return row
}

func setupWriteFile(path string) (*csv.Writer, error) {
	createdFile, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return csv.NewWriter(createdFile), nil
}

func validateFile(fileName string, headers []string) {
	requiredFields := map[string]string{
		"Sat Acct":           "false",
		"Bill Status":        "false",
		"Host Bill From":     "false",
		"Host Bill To":       "false",
		"Transferred kWh":    "false",
		"Banked Prior Month": "false",
		"Allocation %":       "false",
		"Applied":            "false",
		"Banked Carry Over":  "false",
	}

	for i, header := range headers {
		if _, ok := requiredFields[header]; ok {
			switch strings.ToLower(header) {
			case "sat acct":
				if i == 1 {
					requiredFields[header] = "true"
				} else {
					requiredFields[header] = "out of order"
				}
			case "transferred kwh":
				if i == 14 {
					requiredFields[header] = "true"
				} else {
					requiredFields[header] = "out of order"
				}
			case "allocation %":
				if i == 15 {
					requiredFields[header] = "true"
				} else {
					requiredFields[header] = "out of order"
				}
			case "banked prior month":
				if i == 19 {
					requiredFields[header] = "true"
				} else {
					requiredFields[header] = "out of order"
				}
			case "applied":
				if i == 23 {
					requiredFields[header] = "true"
				} else {
					requiredFields[header] = "out of order"
				}
			case "banked carry over":
				if i == 24 {
					requiredFields[header] = "true"
				} else {
					requiredFields[header] = "out of order"
				}
			}
		}

	}

	if !containsHeader {
		validationFileHeaders := append([]string{"File Name"},
			"Sat Acct",
			"Bill Status",
			"Host Bill From",
			"Host Bill To",
			"Transferred kWh",
			"Banked Prior Month",
			"Allocation %",
			"Applied",
			"Banked Carry Over",
		)
		fmt.Println("wg add writeToValidationFile")
		wg.Add(1)
		containsHeader = true
		writeToValidationFile(&validationFileHeaders)
	}

	fmt.Println("wg add writeToValidationFile")
	wg.Add(1)
	writeToValidationFile(&[]string{
		fileName,
		requiredFields["Sat Acct"],
		requiredFields["Bill Status"],
		requiredFields["Host Bill From"],
		requiredFields["Host Bill To"],
		requiredFields["Transferred kWh"],
		requiredFields["Banked Prior Month"],
		requiredFields["Allocation %"],
		requiredFields["Applied"],
		requiredFields["Banked Carry Over"],
	})
}

func writeToAggFile(data *[]string) {
	defer func() {
		fmt.Println("defer writeToAggFile")
		wg.Done()
	}()
	// fmt.Printf("\n:::Writing agg data::: %v", *data)
	mu.Lock()
	err := aggFile.Write(*data)
	if err != nil {
		log.Fatalf("Err AggFile Write::: %v", err)
	}
	mu.Unlock()
}

func writeToValidationFile(data *[]string) {
	defer func() {
		fmt.Println("defer writeToValidationFile")
		wg.Done()
	}()
	// fmt.Printf("\n:::Writing validation data::: %v", *data)
	mu.Lock()
	err := validationFile.Write(*data)
	if err != nil {
		log.Fatalf("Err ValidationFile Write::: %v", err)
	}
	mu.Unlock()
}
