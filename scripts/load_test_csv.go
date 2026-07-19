package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aien-platform/aien/shared/crypto"
	_ "github.com/lib/pq"
)

type IntentPayload struct {
	From   string  `json:"from"`
	To     string  `json:"to"`
	Amount float64 `json:"amount"`
}

type SubmitReq struct {
	Type               string `json:"type"`
	Payload            string `json:"payload"`
	SubmitterID        string `json:"submitter_id"`
	Signature          string `json:"signature"`
	SubmitterPublicKey string `json:"submitter_public_key"`
}

func main() {
	csvPath := flag.String("csv", "data_kaggle/PS_20174392719_1491204439457_log.csv", "Path to the Paysim CSV dataset")
	limit := flag.Int("limit", 1000, "Number of transactions to execute")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	clean := flag.Bool("clean", false, "Clean existing database tables before test")
	flag.Parse()

	fmt.Printf("=== AIEN Kaggle Dataset Load Test ===\n")
	fmt.Printf("CSV Path: %s\n", *csvPath)
	fmt.Printf("Transactions: %d\n", *limit)
	fmt.Printf("Concurrency: %d\n\n", *concurrency)

	// 1. Authenticate with Gateway to get JWT Token
	fmt.Println("[Step 1] Logging in to API Gateway...")
	jwtToken, err := loginToGateway("http://localhost:8081")
	if err != nil {
		logFatalf("Failed to login to API Gateway: %v", err)
	}
	fmt.Println("Successfully logged in. JWT token acquired.")

	// 2. Parse CSV rows
	fmt.Println("[Step 2] Reading CSV dataset...")
	transactions, err := readTransactionsFromCSV(*csvPath, *limit)
	if err != nil {
		logFatalf("Failed to read CSV: %v", err)
	}
	fmt.Printf("Parsed %d TRANSFER transactions from CSV.\n", len(transactions))

	// 3. Connect to Postgres to pre-seed balances
	fmt.Println("[Step 3] Connecting to Postgres database to pre-seed balances...")
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5433/aien?sslmode=disable")
	if err != nil {
		logFatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if *clean {
		fmt.Println("[Step 3.1] Cleaning existing database tables...")
		_, err = db.Exec("TRUNCATE TABLE intents, outbox_events, wallet_transactions CASCADE")
		if err != nil {
			logFatalf("Failed to truncate transaction tables: %v", err)
		}
		_, err = db.Exec("TRUNCATE TABLE wallet_accounts CASCADE")
		if err != nil {
			logFatalf("Failed to truncate wallet accounts: %v", err)
		}
	}

	err = preseedBalances(db, transactions)
	if err != nil {
		logFatalf("Failed to pre-seed balances: %v", err)
	}
	fmt.Println("Balances successfully pre-seeded.")

	// 4. Generate master cryptographic keypair
	fmt.Println("[Step 4] Generating cryptographic keypair...")
	pubKey, privKey, err := crypto.GenerateKeyPair()
	if err != nil {
		logFatalf("Failed to generate keypair: %v", err)
	}
	fmt.Printf("Generated keypair.\nPublic Key: %s\n", pubKey)

	// 5. Submit intents concurrently
	fmt.Println("[Step 5] Submitting transfer intents concurrently...")
	start := time.Now()

	var wg sync.WaitGroup
	jobs := make(chan IntentPayload, len(transactions))
	for _, tx := range transactions {
		jobs <- tx
	}
	close(jobs)

	var successCount int64
	var failCount int64
	var mutex sync.Mutex

	// Create single shared HTTP client with optimized transport to reuse TCP connections
	sharedClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 30 * time.Second,
	}

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tx := range jobs {
				payloadBytes, _ := json.Marshal(tx)
				payloadStr := string(payloadBytes)

				// Create signature
				signBytes := crypto.GetSignBytes("INTENT_TYPE_TRANSFER", tx.From, payloadBytes)
				signature, err := crypto.Sign(signBytes, privKey)
				if err != nil {
					fmt.Printf("Sign error: %v\n", err)
					mutex.Lock()
					failCount++
					mutex.Unlock()
					continue
				}

				reqBody := SubmitReq{
					Type:               "TRANSFER",
					Payload:            payloadStr,
					SubmitterID:        tx.From,
					Signature:          signature,
					SubmitterPublicKey: pubKey,
				}

				bodyBytes, _ := json.Marshal(reqBody)
				req, err := http.NewRequest("POST", "http://localhost:8081/intents", bytes.NewBuffer(bodyBytes))
				if err != nil {
					mutex.Lock()
					failCount++
					mutex.Unlock()
					continue
				}

				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+jwtToken)

				resp, err := sharedClient.Do(req)
				if err != nil {
					fmt.Printf("HTTP Do error: %v\n", err)
					mutex.Lock()
					failCount++
					mutex.Unlock()
					continue
				}

				if resp.StatusCode == http.StatusCreated {
					mutex.Lock()
					successCount++
					mutex.Unlock()
				} else {
					respBodyBytes, _ := io.ReadAll(resp.Body)
					fmt.Printf("HTTP Error status %d: %s\n", resp.StatusCode, string(respBodyBytes))
					mutex.Lock()
					failCount++
					mutex.Unlock()
				}
				resp.Body.Close()
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n=== Test Results ===\n")
	fmt.Printf("Total Time: %v\n", duration)
	fmt.Printf("Requests Sent: %d\n", successCount+failCount)
	fmt.Printf("Successfully Ingested: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failCount)
	fmt.Printf("Throughput: %.2f rps\n", float64(successCount+failCount)/duration.Seconds())
}

func logFatalf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
	os.Exit(1)
}

func loginToGateway(url string) (string, error) {
	loginBody := map[string]string{
		"username": "admin",
		"password": "aien-admin-2026",
	}
	bodyBytes, _ := json.Marshal(loginBody)

	resp, err := http.Post(url+"/api/login", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login failed with status: %d", resp.StatusCode)
	}

	var respJSON map[string]string
	err = json.NewDecoder(resp.Body).Decode(&respJSON)
	if err != nil {
		return "", err
	}

	token, ok := respJSON["token"]
	if !ok {
		return "", fmt.Errorf("token missing in response")
	}

	return token, nil
}

func readTransactionsFromCSV(path string, limit int) ([]IntentPayload, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	headerMap := make(map[string]int)
	for i, h := range headers {
		headerMap[h] = i
	}

	typeIdx, hasType := headerMap["type"]
	amountIdx, hasAmount := headerMap["amount"]
	fromIdx, hasFrom := headerMap["nameOrig"]
	toIdx, hasTo := headerMap["nameDest"]

	if !hasType || !hasAmount || !hasFrom || !hasTo {
		return nil, fmt.Errorf("CSV missing required headers")
	}

	var list []IntentPayload

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if row[typeIdx] != "TRANSFER" {
			continue
		}

		amount, err := strconv.ParseFloat(row[amountIdx], 64)
		if err != nil {
			continue
		}

		list = append(list, IntentPayload{
			From:   row[fromIdx],
			To:     row[toIdx],
			Amount: amount,
		})

		if len(list) >= limit {
			break
		}
	}

	return list, nil
}

func preseedBalances(db *sql.DB, transactions []IntentPayload) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Gather all unique accounts
	uniqueAccounts := make(map[string]bool)
	for _, t := range transactions {
		uniqueAccounts[t.From] = true
		uniqueAccounts[t.To] = true
	}

	stmt, err := tx.Prepare("INSERT INTO wallet_accounts (id, balance) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING")
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Seed with large initial balance (1 billion tokens) to ensure transfers don't fail due to overdrafts
	for acc := range uniqueAccounts {
		_, err = stmt.Exec(acc, 1000000000.0)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
