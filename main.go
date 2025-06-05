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
	"sync"
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
	saleMutex   sync.Mutex
	currentSale int
)

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
		id SERIAL PRIMARY KEY,
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
	initializeSale()

	// Start a goroutine to create a new sale every hour
	go manageSales()
}

func initializeSale() {
	saleMutex.Lock()
	defer saleMutex.Unlock()

	// Check if there's an active sale
	var saleID int
	var startTime time.Time
	var itemsSold int

	err := db.QueryRow("SELECT id, start_time, items_sold FROM sales ORDER BY id DESC LIMIT 1").Scan(&saleID, &startTime, &itemsSold)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("Error checking current sale: %v", err)
	}

	// If no sale exists or the last sale is more than an hour old, create a new one
	if err == sql.ErrNoRows || time.Since(startTime) > 1*time.Hour {
		// Create a new sale
		err = db.QueryRow("INSERT INTO sales (start_time, items_sold) VALUES ($1, 0) RETURNING id", time.Now()).Scan(&saleID)
		if err != nil {
			log.Printf("Error creating new sale: %v", err)
			return
		}
		itemsSold = 0
	}

	currentSale = saleID

	// Initialize Redis counters for the sale
	redisClient.Set(ctx, fmt.Sprintf("sale:%d:total", currentSale), itemsSold, 0)
}

func manageSales() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		<-ticker.C
		initializeSale()
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

	userID := r.URL.Query().Get("user_id")
	itemID := r.URL.Query().Get("id")

	if userID == "" || itemID == "" {
		http.Error(w, "Missing user_id or id param", http.StatusBadRequest)
		return
	}

	// Ensure sale hasn't sold out
	saleMutex.Lock()
	saleID := currentSale
	saleMutex.Unlock()

	// Get current items sold count
	totalSoldStr, err := redisClient.Get(ctx, fmt.Sprintf("sale:%d:total", saleID)).Result()
	if err != nil && err != redis.Nil {
		log.Printf("Error getting total sold: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	totalSold := 0
	if totalSoldStr != "" {
		totalSold, _ = strconv.Atoi(totalSoldStr)
	}

	if totalSold >= MaxItemsPerSale {
		http.Error(w, "Sale sold out", http.StatusConflict)
		return
	}

	// Verify user hasn't exceeded their limit
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	userPurchases, err := redisClient.Get(ctx, userPurchasesKey).Int()
	if err != nil && err != redis.Nil {
		log.Printf("Error getting user purchases: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	userCheckouts, err := redisClient.Get(ctx, userCheckoutsKey).Int()
	if err != nil && err != redis.Nil {
		log.Printf("Error getting user checkouts: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if userPurchases+userCheckouts >= MaxItemsPerUser {
		http.Error(w, fmt.Sprintf("User has reached the maximum limit of %d items", MaxItemsPerUser), http.StatusForbidden)
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

	// Increment user checkouts counter
	redisClient.Incr(ctx, userCheckoutsKey)
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

	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing code param", http.StatusBadRequest)
		return
	}

	// Check code exists in Redis
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

	// Parse code value (format: userID:itemID:saleID)
	codeData := strings.Split(codeValue, ":")
	if len(codeData) != 3 {
		http.Error(w, "Invalid code format", http.StatusBadRequest)
		return
	}

	userID := codeData[0]
	itemID := codeData[1]
	saleID, _ := strconv.Atoi(codeData[2])

	// Check if code has been used
	var used bool
	err = db.QueryRow("SELECT used FROM checkout_attempts WHERE code = $1", code).Scan(&used)
	if err != nil {
		log.Printf("Error checking if code is used: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if used {
		http.Error(w, "Code has already been used", http.StatusConflict)
		return
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// Check if sale is still active and not sold out
	var itemsSold int
	err = tx.QueryRow("SELECT items_sold FROM sales WHERE id = $1", saleID).Scan(&itemsSold)
	if err != nil {
		log.Printf("Error checking sale: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if itemsSold >= MaxItemsPerSale {
		http.Error(w, "Sale sold out", http.StatusConflict)
		return
	}

	// Mark code as used
	_, err = tx.Exec("UPDATE checkout_attempts SET used = true WHERE code = $1", code)
	if err != nil {
		log.Printf("Error marking code as used: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Record the purchase
	_, err = tx.Exec(
		"INSERT INTO purchases (user_id, item_id, code, sale_id, purchased_at) VALUES ($1, $2, $3, $4, $5)",
		userID, itemID, code, saleID, time.Now(),
	)
	if err != nil {
		log.Printf("Error recording purchase: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Update items sold count
	_, err = tx.Exec("UPDATE sales SET items_sold = items_sold + 1 WHERE id = $1", saleID)
	if err != nil {
		log.Printf("Error updating items sold: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Update Redis
	// Increment total sold counter
	redisClient.Incr(ctx, fmt.Sprintf("sale:%d:total", saleID))

	// Increment user purchases counter and decrement checkouts counter
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	redisClient.Incr(ctx, userPurchasesKey)
	redisClient.Expire(ctx, userPurchasesKey, 1*time.Hour)

	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	redisClient.Decr(ctx, userCheckoutsKey)

	// Delete the code from Redis
	redisClient.Del(ctx, codeKey)

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
