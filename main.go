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
	"strconv"
	"strings"
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

var (
	ctx         = context.Background()
	redisClient *redis.Client
	db          *sql.DB
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

local userPurchases = tonumber(redis.call('GET', KEYS[2]) or 0)
local userCheckouts = tonumber(redis.call('GET', KEYS[3]) or 0)
if userPurchases + userCheckouts >= tonumber(ARGV[2]) then
  return {0, "User limit reached"}
end

redis.call('INCR', KEYS[3])
redis.call('EXPIRE', KEYS[3], 3600) -- 1 hour expiration
return {1, "Success"}
`

// Redis Lua script for atomic purchase operation
// KEYS[1] = sale total key (sale:<saleID>:total)
// KEYS[2] = user purchases key (sale:<saleID>:user:<userID>:purchases)
// KEYS[3] = user checkouts key (sale:<saleID>:user:<userID>:checkouts)
// KEYS[4] = code key (code:<code>)
// ARGV[1] = MaxItemsPerSale
const purchaseScript = `
local saleTotal = tonumber(redis.call('GET', KEYS[1]) or 0)
if saleTotal >= tonumber(ARGV[1]) then
  return {0, "Sale sold out"}
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

	fmt.Println("Server starting on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
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

	// Persist the checkout attempt in PostgreSQL
	_, err = db.Exec(
		"INSERT INTO checkout_attempts (user_id, item_id, code, sale_id, created_at) VALUES ($1, $2, $3, $4, $5)",
		userID, itemID, code, saleID, time.Now(),
	)
	if err != nil {
		log.Printf("Error inserting checkout attempt: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Store in Redis with expiration
	redisClient.Set(ctx, fmt.Sprintf("code:%s", code), fmt.Sprintf("%s:%s:%d", userID, itemID, saleID), CodeExpiration)

	// Note: We don't need to increment user checkouts counter here as it was already done by the Lua script
	// Set expiration on the user checkouts key to ensure it expires after 1 hour
	redisClient.Expire(ctx, userCheckoutsKey, 1*time.Hour)

	// Return the code
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
	}, MaxItemsPerSale).Result()

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

	// Begin a transaction to record the purchase and update sale count in the database
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Error beginning transaction: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// Record the purchase
	_, err = tx.Exec(
		"INSERT INTO purchases (user_id, item_id, code, sale_id, purchased_at) VALUES ($1, $2, $3, $4, $5)",
		userID, itemID, code, currentSaleID, time.Now(),
	)
	if err != nil {
		log.Printf("Error inserting purchase: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Update the sale count
	_, err = tx.Exec(
		"UPDATE sales SET items_sold = items_sold + 1 WHERE id = $1",
		currentSaleID,
	)
	if err != nil {
		log.Printf("Error updating sale count: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Generate item details
	item := generateItem(itemID)

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(PurchaseResponse{
		Success:   true,
		Message:   "Purchase successful",
		ItemID:    item.ID,
		ItemName:  item.Name,
		ItemImage: item.Image,
	})
}
