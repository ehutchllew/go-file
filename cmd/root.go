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

const (
	SAT_ACCT           = "Sat Acct"
	BILL_STATUS        = "Bill Status"
	HOST_BILL_FROM     = "Host Bill From"
	HOST_BILL_TO       = "Host Bill To"
	TRANSFERRED_KWH    = "Transferred kWh"
	BANKED_PRIOR_MONTH = "Banked Prior Month"
	ALLOCATION_PERCENT = "Allocation %"
	APPLIED            = "Applied"
	BANKED_CARRY_OVER  = "Banked Carry Over"
)

type ColumnIndex struct {
	FileName           string
	SAT_ACCT           *int
	BILL_STATUS        *int
	HOST_BILL_FROM     *int
	HOST_BILL_TO       *int
	TRANSFERRED_KWH    *int
	BANKED_PRIOR_MONTH *int
	ALLOCATION_PERCENT *int
	APPLIED            *int
	BANKED_CARRY_OVER  *int
}

func (c *ColumnIndex) GetFileHeaders() []string {
	fileHeaders := []string{
		SAT_ACCT,
		BILL_STATUS,
		HOST_BILL_FROM,
		HOST_BILL_TO,
		TRANSFERRED_KWH,
		BANKED_PRIOR_MONTH,
		ALLOCATION_PERCENT,
		APPLIED,
		BANKED_CARRY_OVER,
	}

	return fileHeaders
}

func (c *ColumnIndex) SortWritableData(data *[]string) []string {
	dataValues := *data
	return []string{
		dataValues[*c.SAT_ACCT],
		dataValues[*c.BILL_STATUS],
		dataValues[*c.HOST_BILL_FROM],
		dataValues[*c.HOST_BILL_TO],
		dataValues[*c.TRANSFERRED_KWH],
		dataValues[*c.BANKED_PRIOR_MONTH],
		dataValues[*c.ALLOCATION_PERCENT],
		dataValues[*c.APPLIED],
		dataValues[*c.BANKED_CARRY_OVER],
	}
}

func (c *ColumnIndex) WhichFieldsExist() map[string]string {
	existMap := map[string]string{
		SAT_ACCT:           "false",
		BILL_STATUS:        "false",
		HOST_BILL_FROM:     "false",
		HOST_BILL_TO:       "false",
		TRANSFERRED_KWH:    "false",
		BANKED_PRIOR_MONTH: "false",
		ALLOCATION_PERCENT: "false",
		APPLIED:            "false",
		BANKED_CARRY_OVER:  "false",
	}

	if c.SAT_ACCT != nil {
		existMap[SAT_ACCT] = "true"
	}
	if c.BILL_STATUS != nil {
		existMap[BILL_STATUS] = "true"
	}
	if c.HOST_BILL_FROM != nil {
		existMap[HOST_BILL_FROM] = "true"
	}
	if c.HOST_BILL_TO != nil {
		existMap[HOST_BILL_TO] = "true"
	}
	if c.TRANSFERRED_KWH != nil {
		existMap[TRANSFERRED_KWH] = "true"
	}
	if c.BANKED_PRIOR_MONTH != nil {
		existMap[BANKED_PRIOR_MONTH] = "true"
	}
	if c.ALLOCATION_PERCENT != nil {
		existMap[ALLOCATION_PERCENT] = "true"
	}
	if c.APPLIED != nil {
		existMap[APPLIED] = "true"
	}
	if c.BANKED_CARRY_OVER != nil {
		existMap[BANKED_CARRY_OVER] = "true"
	}

	return existMap
}

var (
	aggFile                      *csv.Writer
	containsValidationFileHeader bool
	containsAggFileHeader        bool
	mu                           sync.Mutex
	wg                           sync.WaitGroup
	validationFile               *csv.Writer
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
	defer wg.Done()

	file, err := excelize.OpenFile(filePath)
	if err != nil {
		log.Fatalf("Cannot open file (%s)\n Err: %v", filePath, err)
	}
	defer file.Close()

	fileName := filepath.Base(filePath)

	fileColumnIndexMap := ColumnIndex{
		FileName: fileName,
	}

	sheets := file.GetSheetList()
	for _, sheet := range sheets {
		rows, err := file.GetRows(sheet)
		if err != nil {
			log.Fatalf("Unable to get rows for sheet (%s)\n Err: %v", sheet, err)
		}

		for i, row := range rows {
			if i == 0 {
				populateIndexMap(row, &fileColumnIndexMap)
				validateFile(&fileColumnIndexMap)

				if !containsAggFileHeader {
					fileHeaders := fileColumnIndexMap.GetFileHeaders()
					wg.Add(1)
					writeToAggFile(&fileHeaders)
					containsAggFileHeader = true
				}
			} else {
				wg.Add(1)
				*ch <- fileColumnIndexMap.SortWritableData(&row)
			}
		}

	}
}

func populateIndexMap(headers []string, fileColumnIndexMap *ColumnIndex) error {
	for i, header := range headers {
		lowerHeader := strings.ToLower(header)
		switch lowerHeader {
		case strings.ToLower(SAT_ACCT):
			fileColumnIndexMap.SAT_ACCT = &i
		case strings.ToLower(BILL_STATUS):
			fileColumnIndexMap.BILL_STATUS = &i
		case strings.ToLower(HOST_BILL_FROM):
			fileColumnIndexMap.HOST_BILL_FROM = &i
		case strings.ToLower(HOST_BILL_TO):
			fileColumnIndexMap.HOST_BILL_TO = &i
		case strings.ToLower(TRANSFERRED_KWH):
			fileColumnIndexMap.TRANSFERRED_KWH = &i
		case strings.ToLower(BANKED_PRIOR_MONTH):
			fileColumnIndexMap.BANKED_PRIOR_MONTH = &i
		case strings.ToLower(ALLOCATION_PERCENT):
			fileColumnIndexMap.ALLOCATION_PERCENT = &i
		case strings.ToLower(APPLIED):
			fileColumnIndexMap.APPLIED = &i
		case strings.ToLower(BANKED_CARRY_OVER):
			fileColumnIndexMap.BANKED_CARRY_OVER = &i
		}
	}

	return nil
}

func readFromChannel(ch *chan []string, done *chan int8) {
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

func setupWriteFile(path string) (*csv.Writer, error) {
	createdFile, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return csv.NewWriter(createdFile), nil
}

func validateFile(fileColumnIndexMap *ColumnIndex) {
	requiredFields := fileColumnIndexMap.WhichFieldsExist()
	if !containsValidationFileHeader {
		validationFileHeaders := append([]string{"File Name"},
			fileColumnIndexMap.GetFileHeaders()...,
		)
		wg.Add(1)
		containsValidationFileHeader = true
		writeToValidationFile(&validationFileHeaders)
	}

	wg.Add(1)
	writeToValidationFile(&[]string{
		fileColumnIndexMap.FileName,
		requiredFields[SAT_ACCT],
		requiredFields[BILL_STATUS],
		requiredFields[HOST_BILL_FROM],
		requiredFields[HOST_BILL_TO],
		requiredFields[TRANSFERRED_KWH],
		requiredFields[BANKED_PRIOR_MONTH],
		requiredFields[ALLOCATION_PERCENT],
		requiredFields[APPLIED],
		requiredFields[BANKED_CARRY_OVER],
	})
}

func writeToAggFile(data *[]string) {
	defer wg.Done()
	// fmt.Printf("\n:::Writing agg data::: %v", *data)
	mu.Lock()
	err := aggFile.Write(*data)
	if err != nil {
		log.Fatalf("Err AggFile Write::: %v", err)
	}
	mu.Unlock()
}

func writeToValidationFile(data *[]string) {
	defer wg.Done()
	// fmt.Printf("\n:::Writing validation data::: %v", *data)
	mu.Lock()
	err := validationFile.Write(*data)
	if err != nil {
		log.Fatalf("Err ValidationFile Write::: %v", err)
	}
	mu.Unlock()
}
