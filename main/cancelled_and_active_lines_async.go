package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/agavegno/soporte/utils"
)

const (
	goroutines = 8
)

func main() {
	cores := runtime.NumCPU()
	fmt.Printf("This machine has %d CPU cores. \n", cores)

	fileIn, err := os.Open("express_money_cancelled.csv")
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(fileIn)
	rows, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Create("result_cancelled.csv")
	defer file.Close()
	if err != nil {
		log.Fatalln("failed to open file", err)
	}

	w := csv.NewWriter(file)
	defer w.Flush()

	log.Println(fmt.Sprintf("Iniciando proceso %s - cantidad de usuarios a procesar: %d", time.Now(), len(rows)))

	inputCh := make(chan map[string]string)
	resultCh := make(chan []string)

	go load(rows, inputCh)

	go process(inputCh, resultCh, goroutines)

	for v := range resultCh {
		if err := w.Write(v); err != nil {
			log.Println(fmt.Sprintf("%s;%s", v[0], v[1]))
		}
	}
	log.Println(fmt.Sprintf("Fin de proceso %s:", time.Now()))
}

func load(rows [][]string, inputCh chan<- map[string]string) {
	for _, row := range rows {
		creditLineID := row[0]
		borrowerID := row[1]
		inputCh <- map[string]string{creditLineID: borrowerID}
	}
	close(inputCh)
}

func process(inputCh <-chan map[string]string, resultCh chan<- []string, goroutines int) {
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		log.Println("goroutine", i)
		go func() {
			for v := range inputCh {
				func(value map[string]string) {
					for creditLineID, borrowerID := range value {
						log.Println("processing:", creditLineID, borrowerID)
						hasActiveLoans, err := cancelledCreditLineHasActiveLoans(creditLineID, borrowerID)
						if err != nil {
							log.Fatal(err)
						}
						if hasActiveLoans {
							creditLines, err := searchActivesCreditLines(borrowerID)
							if err != nil {
								log.Fatal(err)
							}
							for _, creditLineID := range creditLines {
								err := cancelCreditLine(creditLineID, borrowerID)
								if err != nil {
									log.Fatal(err)
								}
								resultCh <- []string{strconv.FormatInt(creditLineID, utils.Base), borrowerID}
							}
						}
					}
				}(v)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	close(resultCh)
}

func cancelledCreditLineHasActiveLoans(creditLineID string, borrowerID string) (bool, error) {
	client := &http.Client{}
	url := fmt.Sprintf("%s/search?offset=0&limit=100&sort=date_created&status=CREDITED,APPROVED,ON_TIME,OVERDUE&borrower_id=%s&credit_line_id=%s",
		utils.BasePathLoans, borrowerID, creditLineID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		errMsg := fmt.Sprintf("loans search request creation error (credit line %s) - %s", creditLineID, err)
		fmt.Println(errMsg)
		return false, err
	}
	req.Header.Add("x-caller-scopes", "admin")
	req.Header.Add("x-caller-id", "credits-admin-api")

	resp, err := client.Do(req)
	if err != nil {
		errMsg := fmt.Sprintf("Loans Search Error (CL %s) - %s", creditLineID, err)
		fmt.Println(errMsg)
		return false, err
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		err := fmt.Errorf("Loans search error - %d - %s - %s", resp.StatusCode, url, string(bodyBytes))
		return false, err
	}

	loans := utils.PagingLoans{}
	err = json.Unmarshal(bodyBytes, &loans)
	if err != nil {
		log.Fatal(err)
	}

	if len(loans.Results) > 0 {
		return true, nil
	}

	return false, nil
}

func searchActivesCreditLines(borrowerID string) ([]int64, error) {
	var creditLines []int64
	client := &http.Client{}
	url := fmt.Sprintf("%s/search?caller.id=%s&borrower_id=%s&product=express_money&status=PENDING,APPROVED,REJECTED",
		utils.BasePathCreditLine, borrowerID, borrowerID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		errMsg := fmt.Sprintf("search credit line for borrower %s get request creation error - %s", borrowerID, err)
		fmt.Println(errMsg)
		return creditLines, err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		errMsg := fmt.Sprintf("search CL - borrower %s Get Error - %s", borrowerID, err)
		fmt.Println(errMsg)
		return creditLines, err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		err := fmt.Errorf("search CL get error - %s - %d - %s - %s", borrowerID, resp.StatusCode, url, string(body))
		return creditLines, err
	}

	result := &utils.PagingCreditLines{}
	json.Unmarshal(body, result)

	if len(result.Results) > 0 {
		for _, creditLine := range result.Results {
			creditLines = append(creditLines, creditLine.ID)
		}
	}

	return creditLines, nil
}

func cancelCreditLine(creditLineID int64, borrowerID string) error {
	client := &http.Client{}
	url := fmt.Sprintf("%s/%d?caller.id=%s", utils.BasePathCreditLine, creditLineID, borrowerID)

	bodyMap := make(map[string]interface{})
	bodyMap["status"] = "cancelled"
	bodyMap["status_detail"] = "proposal_mistake"

	body, err := json.Marshal(bodyMap)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(body))
	if err != nil {
		errMsg := fmt.Sprintf("credit line %s put request creation error - %s", creditLineID, err)
		fmt.Println(errMsg)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		errMsg := fmt.Sprintf("CL %s Put Error - %s", creditLineID, err)
		fmt.Println(errMsg)
		return err
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		err := fmt.Errorf("CL put error - %d - %s - %s", resp.StatusCode, url, string(bodyBytes))
		return err
	}

	creditLine := &utils.CreditLine{}
	json.Unmarshal(bodyBytes, creditLine)

	if creditLine.Status != "cancelled" {
		return fmt.Errorf("Status put error, actual status: %s", creditLine.Status)
	}

	return nil
}
