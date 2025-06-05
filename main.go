package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// Some variables and constants
const (
	MaxItemsPerSale = 10000
	MaxItemsPerUser = 10
	CodeExpiration  = 15 * time.Minute
)

// Job types for background processing
type CheckoutJob struct {
	UserID    string
	ItemID    string
	Code      string
	SaleID    int
	CreatedAt time.Time
}

type PurchaseJob struct {
	UserID      string
	ItemID      string
	Code        string
	SaleID      int
	PurchasedAt time.Time
}

var (
	ctx             = context.Background()
	redisClient     *redis.Client
	db              *sql.DB
	checkoutJobChan = make(chan CheckoutJob, 1000) // Buffer size of 1000
	purchaseJobChan = make(chan PurchaseJob, 1000) // Buffer size of 1000
	wg              sync.WaitGroup                 // For graceful shutdown
)

// Redis Lua script for atomic checkout validation and counter increment
// KEYS[1] = sale total key (sale:<saleID>:total)
// KEYS[2] = user purchases key (sale:<saleID>:user:<userID>:purchases)
// KEYS[3] = user checkouts key (sale:<saleID>:user:<userID>:checkouts)
// ARGV[1] = MaxItemsPerSale
// ARGV[2] = MaxItemsPerUser
const checkoutScript = `
local saleTotal = tonumber(redis.call('GET', KEYS[1]) or 0)
if saleTotal >= tonumber(ARGV[1]) then
  return {0, "Sale sold out"}
end

-- Only check purchases against the limit, not checkouts
local userPurchases = tonumber(redis.call('GET', KEYS[2]) or 0)
if userPurchases >= tonumber(ARGV[2]) then
  return {0, "User limit reached"}
end

-- Still track checkouts for informational purposes
redis.call('INCR', KEYS[3])
redis.call('EXPIRE', KEYS[3], 3600) -- 1 hour expiration
return {1, "Success"}
`

// Redis Lua script for atomic purchase operations
// KEYS[1] = sale total key (sale:<saleID>:total)
// KEYS[2] = user purchases key (sale:<saleID>:user:<userID>:purchases)
// KEYS[3] = user checkouts key (sale:<saleID>:user:<userID>:checkouts)
// KEYS[4] = code key (code:<code>)
// ARGV[1] = MaxItemsPerSale
// ARGV[2] = MaxItemsPerUser
const purchaseScript = `
local saleTotal = tonumber(redis.call('GET', KEYS[1]) or 0)
if saleTotal >= tonumber(ARGV[1]) then
  return {0, "Sale sold out"}
end

-- Check if user has reached their purchase limit
local userPurchases = tonumber(redis.call('GET', KEYS[2]) or 0)
if userPurchases >= tonumber(ARGV[2]) then
  return {0, "User purchase limit reached"}
end

-- Increment sale total counter
redis.call('INCR', KEYS[1])

-- Increment user purchases counter
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], 3600) -- 1 hour expiration

-- Decrement user checkouts counter
local checkouts = tonumber(redis.call('GET', KEYS[3]) or 0)
if checkouts > 0 then
  redis.call('DECR', KEYS[3])
end

-- Delete the code
redis.call('DEL', KEYS[4])

return {1, "Success"}
`

type CheckoutResponse struct {
	Code string `json:"code"`
}

type PurchaseResponse struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	ItemID    string `json:"item_id,omitempty"`
	ItemName  string `json:"item_name,omitempty"`
	ItemImage string `json:"item_image,omitempty"`
}

type Item struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Image string `json:"image"`
}

// processCheckoutJobs handles the background processing of checkout attempts
func processCheckoutJobs() {
	defer wg.Done()
	for job := range checkoutJobChan {
		// Persist the checkout attempt in PostgreSQL
		_, err := db.Exec(
			"INSERT INTO checkout_attempts (user_id, item_id, code, sale_id, created_at) VALUES ($1, $2, $3, $4, $5)",
			job.UserID, job.ItemID, job.Code, job.SaleID, job.CreatedAt,
		)
		if err != nil {
			log.Printf("Error inserting checkout attempt: %v", err)
			// Retry logic - we'll retry up to 3 times with exponential backoff
			retryCheckoutJob(job, 1)
		}
	}
}

// retryCheckoutJob attempts to retry a failed checkout job with exponential backoff
func retryCheckoutJob(job CheckoutJob, attempt int) {
	if attempt > 3 {
		log.Printf("Failed to insert checkout attempt after 3 retries: %s, %s, %s", job.UserID, job.ItemID, job.Code)
		return
	}

	// Exponential backoff: 1s, 2s, 4s
	backoff := time.Duration(1<<(attempt-1)) * time.Second
	time.Sleep(backoff)

	_, err := db.Exec(
		"INSERT INTO checkout_attempts (user_id, item_id, code, sale_id, created_at) VALUES ($1, $2, $3, $4, $5)",
		job.UserID, job.ItemID, job.Code, job.SaleID, job.CreatedAt,
	)
	if err != nil {
		log.Printf("Retry %d failed for checkout attempt: %v", attempt, err)
		retryCheckoutJob(job, attempt+1)
	} else {
		log.Printf("Successfully inserted checkout attempt on retry %d", attempt)
	}
}

// processPurchaseJobs handles the background processing of purchases
func processPurchaseJobs() {
	defer wg.Done()
	for job := range purchaseJobChan {
		processPurchase(job, 0)
	}
}

// processPurchase processes a purchase job with retry logic
func processPurchase(job PurchaseJob, attempt int) {
	// Max retries
	if attempt > 3 {
		log.Printf("Failed to process purchase after 3 retries: %s, %s, %s", job.UserID, job.ItemID, job.Code)
		return
	}

	// If this is a retry, add exponential backoff
	if attempt > 0 {
		backoff := time.Duration(1<<(attempt-1)) * time.Second
		time.Sleep(backoff)
	}

	// Begin a transaction to record the purchase and update sale count
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Error beginning transaction: %v", err)
		processPurchase(job, attempt+1)
		return
	}

	// Record the purchase
	_, err = tx.Exec(
		"INSERT INTO purchases (user_id, item_id, code, sale_id, purchased_at) VALUES ($1, $2, $3, $4, $5)",
		job.UserID, job.ItemID, job.Code, job.SaleID, job.PurchasedAt,
	)
	if err != nil {
		tx.Rollback()
		log.Printf("Error inserting purchase: %v", err)
		processPurchase(job, attempt+1)
		return
	}

	// Update the sale count
	_, err = tx.Exec(
		"UPDATE sales SET items_sold = items_sold + 1 WHERE id = $1",
		job.SaleID,
	)
	if err != nil {
		tx.Rollback()
		log.Printf("Error updating sale count: %v", err)
		processPurchase(job, attempt+1)
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		processPurchase(job, attempt+1)
		return
	}

	if attempt > 0 {
		log.Printf("Successfully processed purchase on retry %d", attempt)
	}
}

// reconcileRedisWithDB periodically checks for mismatches between Redis and the database
func reconcileRedisWithDB() {
	defer wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Get the current sale ID
			saleID := currentSaleID()

			// Reconcile sale total count
			reconcileSaleTotal(saleID)

			// Check for codes in Redis that aren't in the database
			reconcileCodes(saleID)

			// Check for checkout attempts in the database that aren't in Redis
			reconcileCheckouts(saleID)
		case <-ctx.Done():
			return
		}
	}
}

// reconcileCheckouts checks for checkout attempts in the database that aren't in Redis
func reconcileCheckouts(saleID int) {
	// Get recent checkout attempts from the database that aren't marked as used
	rows, err := db.Query(
		"SELECT code, user_id, item_id FROM checkout_attempts WHERE sale_id = $1 AND used = FALSE AND created_at > NOW() - INTERVAL '15 minutes'",
		saleID,
	)
	if err != nil {
		log.Printf("Error getting recent checkout attempts: %v", err)
		return
	}
	defer rows.Close()

	// Check each code
	for rows.Next() {
		var code, userID, itemID string
		if err := rows.Scan(&code, &userID, &itemID); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Check if the code exists in Redis
		codeKey := fmt.Sprintf("code:%s", code)
		exists, err := redisClient.Exists(ctx, codeKey).Result()
		if err != nil {
			log.Printf("Error checking if code exists in Redis: %v", err)
			continue
		}

		// If the code doesn't exist in Redis but should (within TTL period), add it
		if exists == 0 {
			log.Printf("Found code in DB not in Redis: %s, adding to Redis", code)

			// Set the code in Redis with the original TTL
			codeValue := fmt.Sprintf("%s:%s:%d", userID, itemID, saleID)
			err = redisClient.Set(ctx, codeKey, codeValue, CodeExpiration).Err()
			if err != nil {
				log.Printf("Error setting code in Redis: %v", err)
				continue
			}

			// Increment the user's checkout count
			userCheckoutKey := fmt.Sprintf("user:%s:sale:%d:checkouts", userID, saleID)
			err = redisClient.Incr(ctx, userCheckoutKey).Err()
			if err != nil {
				log.Printf("Error incrementing user checkout count: %v", err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating over rows: %v", err)
	}
}

// reconcileSaleTotal ensures the sale total in Redis matches the database
func reconcileSaleTotal(saleID int) {
	// Get the sale total from the database
	var dbTotal int
	err := db.QueryRow("SELECT items_sold FROM sales WHERE id = $1", saleID).Scan(&dbTotal)
	if err != nil {
		log.Printf("Error getting sale total from database: %v", err)
		return
	}

	// Get the sale total from Redis
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	redisTotal, err := redisClient.Get(ctx, saleKey).Int()
	if err != nil && err != redis.Nil {
		log.Printf("Error getting sale total from Redis: %v", err)
		return
	}

	// If Redis total is nil, set it to the database total
	if err == redis.Nil {
		redisClient.Set(ctx, saleKey, dbTotal, 0)
		return
	}

	// If there's a mismatch, update Redis to match the database
	if redisTotal != dbTotal {
		log.Printf("Reconciling sale total: Redis=%d, DB=%d", redisTotal, dbTotal)
		redisClient.Set(ctx, saleKey, dbTotal, 0)
	}
}

// reconcileCodes checks for codes in Redis that aren't in the database
func reconcileCodes(saleID int) {
	// Get all code keys for the current sale
	keys, err := redisClient.Keys(ctx, "code:*").Result()
	if err != nil {
		log.Printf("Error getting code keys from Redis: %v", err)
		return
	}

	// Check each code
	for _, key := range keys {
		// Extract the code from the key
		code := strings.TrimPrefix(key, "code:")

		// Get the code value
		codeValue, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			continue // Code might have expired
		}

		// Parse the code value
		parts := strings.Split(codeValue, ":")
		if len(parts) != 3 {
			continue
		}

		userID := parts[0]
		itemID := parts[1]
		codeSaleID, err := strconv.Atoi(parts[2])
		if err != nil {
			continue
		}

		// Only check codes for the current sale
		if codeSaleID != saleID {
			continue
		}

		// Check if the code exists in the database
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM checkout_attempts WHERE code = $1", code).Scan(&count)
		if err != nil {
			log.Printf("Error checking code in database: %v", err)
			continue
		}

		// If the code doesn't exist in the database, add it
		if count == 0 {
			log.Printf("Found code in Redis not in DB: %s, adding to database", code)
			_, err = db.Exec(
				"INSERT INTO checkout_attempts (user_id, item_id, code, sale_id, created_at) VALUES ($1, $2, $3, $4, $5)",
				userID, itemID, code, saleID, time.Now(),
			)
			if err != nil {
				log.Printf("Error inserting missing code into database: %v", err)
			}
		}
	}

	// Also check for missing purchases in the database
	reconcilePurchases(saleID)
}

// reconcilePurchases checks for purchases that haven't been recorded in the database
func reconcilePurchases(saleID int) {
	// Get all user purchase counts from Redis
	userPurchaseKeys, err := redisClient.Keys(ctx, fmt.Sprintf("user:*:sale:%d:purchases", saleID)).Result()
	if err != nil {
		log.Printf("Error getting user purchase keys from Redis: %v", err)
		return
	}

	// Check each user's purchases
	for _, key := range userPurchaseKeys {
		// Extract the user ID from the key
		// Format: user:<userID>:sale:<saleID>:purchases
		parts := strings.Split(key, ":")
		if len(parts) != 5 {
			continue
		}
		userID := parts[1]

		// Get the user's purchase count from Redis
		redisPurchaseCount, err := redisClient.Get(ctx, key).Int()
		if err != nil {
			log.Printf("Error getting purchase count from Redis for user %s: %v", userID, err)
			continue
		}

		// Get the user's purchase count from the database
		var dbPurchaseCount int
		err = db.QueryRow(
			"SELECT COUNT(*) FROM purchases WHERE user_id = $1 AND sale_id = $2",
			userID, saleID,
		).Scan(&dbPurchaseCount)
		if err != nil {
			log.Printf("Error getting purchase count from database for user %s: %v", userID, err)
			continue
		}

		// If there's a mismatch, check for unused codes that should be marked as used
		if redisPurchaseCount > dbPurchaseCount {
			log.Printf("Purchase count mismatch for user %s: Redis=%d, DB=%d", userID, redisPurchaseCount, dbPurchaseCount)

			// Get the user's checkout attempts that aren't marked as used
			rows, err := db.Query(
				"SELECT code, item_id FROM checkout_attempts WHERE user_id = $1 AND sale_id = $2 AND used = FALSE",
				userID, saleID,
			)
			if err != nil {
				log.Printf("Error getting unused checkout attempts: %v", err)
				continue
			}
			defer rows.Close()

			// Mark codes as used and create purchase records until counts match
			neededPurchases := redisPurchaseCount - dbPurchaseCount
			for rows.Next() && neededPurchases > 0 {
				var code, itemID string
				if err := rows.Scan(&code, &itemID); err != nil {
					log.Printf("Error scanning row: %v", err)
					continue
				}

				// Start a transaction
				tx, err := db.Begin()
				if err != nil {
					log.Printf("Error starting transaction: %v", err)
					continue
				}

				// Mark the code as used
				_, err = tx.Exec(
					"UPDATE checkout_attempts SET used = TRUE WHERE code = $1",
					code,
				)
				if err != nil {
					tx.Rollback()
					log.Printf("Error marking code as used: %v", err)
					continue
				}

				// Insert a purchase record
				_, err = tx.Exec(
					"INSERT INTO purchases (user_id, item_id, code, sale_id, purchased_at) VALUES ($1, $2, $3, $4, $5)",
					userID, itemID, code, saleID, time.Now(),
				)
				if err != nil {
					tx.Rollback()
					log.Printf("Error inserting purchase record: %v", err)
					continue
				}

				// Update the sale count
				_, err = tx.Exec(
					"UPDATE sales SET items_sold = items_sold + 1 WHERE id = $1",
					saleID,
				)
				if err != nil {
					tx.Rollback()
					log.Printf("Error updating sale count: %v", err)
					continue
				}

				// Commit the transaction
				if err := tx.Commit(); err != nil {
					log.Printf("Error committing transaction: %v", err)
					continue
				}

				log.Printf("Reconciled purchase for user %s, code %s", userID, code)
				neededPurchases--
			}
		}
	}
}

func init() {
	// Initialize Redis client
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr: redisURL,
	})

	// Test Redis connection
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	// Initialize PostgreSQL connection
	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		postgresURL = "postgres://postgres:postgres@localhost/flashsale?sslmode=disable"
	}

	db, err = sql.Open("postgres", postgresURL)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}

	// Create necessary tables if they don't exist
	createTablesSQL := `
	CREATE TABLE IF NOT EXISTS sales (
		id INT PRIMARY KEY, -- Using epoch timestamp as ID
		start_time TIMESTAMP NOT NULL,
		items_sold INT DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS checkout_attempts (
		id SERIAL PRIMARY KEY,
		user_id TEXT NOT NULL,
		item_id TEXT NOT NULL,
		code TEXT UNIQUE NOT NULL,
		sale_id INT NOT NULL,
		created_at TIMESTAMP NOT NULL,
		used BOOLEAN DEFAULT FALSE
	);

	CREATE TABLE IF NOT EXISTS purchases (
		id SERIAL PRIMARY KEY,
		user_id TEXT NOT NULL,
		item_id TEXT NOT NULL,
		code TEXT NOT NULL,
		sale_id INT NOT NULL,
		purchased_at TIMESTAMP NOT NULL
	);
	`

	_, err = db.Exec(createTablesSQL)
	if err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	// Initialize current sale
	saleID := currentSaleID()
	ensureSaleExists(saleID)

	// Start background workers
	wg.Add(3) // One for checkout jobs, one for purchase jobs, one for reconciliation
	go processCheckoutJobs()
	go processPurchaseJobs()
	go reconcileRedisWithDB()

	// Start a goroutine to create a new sale every hour
	go manageSales()
}

// currentSaleID returns the current sale ID based on the UTC hour boundary
// This is a stateless function that doesn't require a mutex
func currentSaleID() int {
	// Get current time in UTC
	now := time.Now().UTC()
	// Truncate to the start of the current hour
	hourStart := now.Truncate(time.Hour)
	// Return Unix timestamp (seconds since epoch)
	return int(hourStart.Unix())
}

// ensureSaleExists makes sure the sale record exists in the database
// and initializes Redis counters if needed
func ensureSaleExists(saleID int) error {
	// Check if there's already a sale for this hour
	var itemsSold int
	err := db.QueryRow("SELECT items_sold FROM sales WHERE id = $1", saleID).Scan(&itemsSold)

	// If no sale exists for this hour, create a new one
	if err == sql.ErrNoRows {
		// Create a new sale with the epoch as the ID
		_, err = db.Exec("INSERT INTO sales (id, start_time, items_sold) VALUES ($1, $2, 0)",
			saleID, time.Unix(int64(saleID), 0))
		if err != nil {
			log.Printf("Error creating new sale: %v", err)
			return err
		}
		itemsSold = 0
	} else if err != nil {
		log.Printf("Error checking current sale: %v", err)
		return err
	}

	// Initialize Redis counters for the sale if they don't exist
	// Using SET NX to only set if the key doesn't exist
	redisClient.SetNX(ctx, fmt.Sprintf("sale:%d:total", saleID), itemsSold, 0)
	return nil
}

func manageSales() {
	for {
		// Calculate time until the next hour boundary
		now := time.Now().UTC()
		nextHour := now.Truncate(time.Hour).Add(time.Hour)
		duration := nextHour.Sub(now)

		// Sleep until the next hour boundary
		time.Sleep(duration)

		// Get the sale ID for the new hour
		saleID := currentSaleID()

		// Ensure the sale exists in the database
		if err := ensureSaleExists(saleID); err != nil {
			log.Printf("Error ensuring sale exists: %v", err)
		}
	}
}

func generateItem(itemID string) Item {
	itemNames := []string{"boxlogo", "not for climbing", "physics", "4 hounds", "not blank", "not or never", "nobody knows", "out of nothing"}
	// categories := []string{"hoodie", "hoodie", "hoodie", "longsleeve", "longsleeve", "longsleeve", "t-shirt", "t-shirt"}

	// Generate a random index based on itemID
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(itemNames))

	// Generate a random image index (1-8) for the CDN URL
	imageIndex := rand.Intn(8) + 1
	imageURL := fmt.Sprintf("https://not-contest-cdn.openbuilders.xyz/items/%d.jpg", imageIndex)

	// IDK y but, 6th image has a PNG extention (so use `.png` if `imageIdnex` is 6th)
	if imageIndex == 6 {
		imageURL = fmt.Sprintf("https://not-contest-cdn.openbuilders.xyz/items/%d.png", imageIndex)
	}

	return Item{
		ID:    itemID,
		Name:  itemNames[index],
		Image: imageURL,
	}
}

func main() {
	http.HandleFunc("/checkout", checkoutHandler)
	http.HandleFunc("/purchase", purchaseHandler)

	// Set up graceful shutdown
	server := &http.Server{Addr: ":8080"}

	// Start the server in a goroutine
	go func() {
		fmt.Println("Server starting on :8080...")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Shutdown the server
	fmt.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	// Close job channels and wait for workers to finish
	close(checkoutJobChan)
	close(purchaseJobChan)
	wg.Wait()

	fmt.Println("Server gracefully stopped")
}

func checkoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the request body
	var req struct {
		UserID string `json:"user_id"`
		ItemID string `json:"item_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	userID := req.UserID
	itemID := req.ItemID

	if userID == "" || itemID == "" {
		http.Error(w, "User ID and Item ID are required", http.StatusBadRequest)
		return
	}

	// Get the current sale ID using the stateless function
	saleID := currentSaleID()

	// Ensure the sale exists in the database
	if err := ensureSaleExists(saleID); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Use Redis Lua script for atomic checkout validation and counter increment
	ctx := context.Background()
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)

	// Execute the checkout script
	result, err := redisClient.Eval(ctx, checkoutScript, []string{
		saleKey,
		userPurchasesKey,
		userCheckoutsKey,
	}, MaxItemsPerSale, MaxItemsPerUser).Result()

	if err != nil {
		log.Printf("Error executing checkout script: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Check the result
	resultArray, ok := result.([]interface{})
	if !ok || len(resultArray) < 2 {
		log.Printf("Invalid result from checkout script: %v", result)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	success, _ := resultArray[0].(int64)
	message, _ := resultArray[1].(string)

	if success != 1 {
		http.Error(w, message, http.StatusBadRequest)
		return
	}

	// Generate a unique one-time-use code
	code := uuid.New().String()

	// Store in Redis with expiration
	redisClient.Set(ctx, fmt.Sprintf("code:%s", code), fmt.Sprintf("%s:%s:%d", userID, itemID, saleID), CodeExpiration)

	// Set expiration on the user checkouts key to ensure it expires after 1 hour
	redisClient.Expire(ctx, userCheckoutsKey, 1*time.Hour)

	// Send the checkout job to the background worker
	checkoutJobChan <- CheckoutJob{
		UserID:    userID,
		ItemID:    itemID,
		Code:      code,
		SaleID:    saleID,
		CreatedAt: time.Now(),
	}

	// Return the code immediately
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(CheckoutResponse{Code: code})
}

func purchaseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the current sale ID using the stateless function
	currentSaleID := currentSaleID()

	// Ensure the sale exists in the database
	if err := ensureSaleExists(currentSaleID); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing code param", http.StatusBadRequest)
		return
	}

	// Check if the code exists in Redis
	ctx := context.Background()
	codeKey := fmt.Sprintf("code:%s", code)
	codeValue, err := redisClient.Get(ctx, codeKey).Result()
	if err == redis.Nil {
		http.Error(w, "Invalid or expired code", http.StatusBadRequest)
		return
	} else if err != nil {
		log.Printf("Error checking code: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Parse the code value to get userID, itemID, and saleID
	parts := strings.Split(codeValue, ":")
	if len(parts) != 3 {
		http.Error(w, "Invalid code format", http.StatusBadRequest)
		return
	}

	userID := parts[0]
	itemID := parts[1]
	codeSaleID, err := strconv.Atoi(parts[2])
	if err != nil {
		http.Error(w, "Invalid code format", http.StatusBadRequest)
		return
	}

	// Verify the code is for the current sale
	if codeSaleID != currentSaleID {
		http.Error(w, "Code is from a previous sale window", http.StatusBadRequest)
		return
	}

	// Use Redis Lua script for atomic purchase operation
	saleKey := fmt.Sprintf("sale:%d:total", currentSaleID)
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", currentSaleID, userID)
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", currentSaleID, userID)

	// Execute the purchase script
	result, err := redisClient.Eval(ctx, purchaseScript, []string{
		saleKey,
		userPurchasesKey,
		userCheckoutsKey,
		codeKey,
	}, MaxItemsPerSale, MaxItemsPerUser).Result()

	if err != nil {
		log.Printf("Error executing purchase script: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Check the result
	resultArray, ok := result.([]interface{})
	if !ok || len(resultArray) < 2 {
		log.Printf("Invalid result from purchase script: %v", result)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	success, _ := resultArray[0].(int64)
	message, _ := resultArray[1].(string)

	if success != 1 {
		http.Error(w, message, http.StatusBadRequest)
		return
	}

	// Send the purchase job to the background worker
	purchaseJobChan <- PurchaseJob{
		UserID:      userID,
		ItemID:      itemID,
		Code:        code,
		SaleID:      currentSaleID,
		PurchasedAt: time.Now(),
	}

	// Generate item details
	item := generateItem(itemID)

	// Return success response immediately
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(PurchaseResponse{
		Success:   true,
		Message:   "Purchase successful",
		ItemID:    item.ID,
		ItemName:  item.Name,
		ItemImage: item.Image,
	})
}
