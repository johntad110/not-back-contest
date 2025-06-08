package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
	"github.com/user/not-contest/internal/service"
)

// PurchaseHandler handles HTTP requests for the /purchase endpoint.
// It processes the second phase of a flash sale transaction, where a user
// attempts to convert a previously generated checkout code into a confirmed purchase.
// It validates the code, applies sale and user purchase limits atomically via Redis,
// and enqueues a background job for persistent storage of the purchase.
type PurchaseHandler struct {
	// RedisManager provides methods for interacting with Redis,
	// specifically for atomic purchase operations and code validation.
	RedisManager *db.RedisManager
	// PostgresManager provides methods for interacting with PostgreSQL.
	// Not directly used here, but a dependency for SaleService.
	PostgresManager *db.PostgresManager
	// SaleService provides business logic related to sale management,
	// such as retrieving the current sale ID and generating item details.
	SaleService *service.SaleService
	// PurchaseChan is a channel used to send asynchronous purchase jobs
	// to a background worker for persistent storage in PostgreSQL.
	PurchaseChan chan<- models.PurchaseJob
}

// NewPurchaseHandler creates and returns a new instance of PurchaseHandler.
// It takes necessary dependencies such as RedisManager, PostgresManager,
// SaleService, and a channel for purchase jobs.
func NewPurchaseHandler(
	redisManager *db.RedisManager,
	postgresManager *db.PostgresManager,
	saleService *service.SaleService,
	purchaseChan chan<- models.PurchaseJob,
) *PurchaseHandler {
	return &PurchaseHandler{
		RedisManager:    redisManager,
		PostgresManager: postgresManager,
		SaleService:     saleService,
		PurchaseChan:    purchaseChan,
	}
}

// Handle processes incoming HTTP requests for the /purchase endpoint.
// It expects a POST request with a `code` query parameter.
//
// The flow involves:
//  1. Validating the HTTP method and the presence of the `code` parameter.
//  2. Retrieving and parsing the details associated with the provided code from Redis.
//  3. Verifying that the code belongs to the current active sale.
//  4. Atomically executing the purchase logic using a Redis Lua script,
//     which decrements counts, increments purchase totals, and deletes the code.
//  5. If the purchase is successful in Redis, enqueueing a `PurchaseJob`
//     to a background worker for asynchronous persistence in PostgreSQL.
//  6. Generating item details for the response.
//  7. Responding to the client with purchase confirmation and item details.
//
// Errors are handled by sending appropriate HTTP status codes and messages.
func (h *PurchaseHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Only allow POST requests for purchase.
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract the checkout code from the URL query parameter.
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing code param", http.StatusBadRequest)
		return
	}

	// Retrieve the value associated with the checkout code from Redis.
	// This value contains the original user ID, item ID, and sale ID.
	codeValue, err := h.RedisManager.GetCodeValue(code)
	if err != nil {
		log.Printf("ERROR: Failed to get code value from Redis for code %s: %v", code, err)
		http.Error(w, "Invalid or expired code", http.StatusBadRequest)
		return
	}

	// Parse the `codeValue` string, which is expected in "userID:itemID:saleID" format.
	parts := strings.Split(codeValue, ":")
	if len(parts) != 3 {
		log.Printf("ERROR: Invalid code value format in Redis for code %s: %s", code, codeValue)
		http.Error(w, "Invalid code format", http.StatusBadRequest)
		return
	}

	userID := parts[0]
	itemID := parts[1]
	saleID, err := strconv.Atoi(parts[2]) // Convert sale ID string to integer.
	if err != nil {
		log.Printf("ERROR: Failed to parse sale ID from code value '%s' for code %s: %v", parts[2], code, err)
		http.Error(w, "Invalid code format", http.StatusBadRequest)
		return
	}

	// Verify that the sale ID embedded in the code matches the currently active sale.
	// This prevents purchases from old or inactive sales using a valid but outdated code.
	currentSaleID, err := h.SaleService.GetCurrentSaleID()
	if err != nil {
		log.Printf("ERROR: Failed to get current sale ID for purchase with code %s: %v", code, err)
		http.Error(w, "Failed to get current sale", http.StatusInternalServerError)
		return
	}

	if saleID != currentSaleID {
		log.Printf("INFO: Purchase attempt with code %s failed. Code is from sale %d, current sale is %d.", code, saleID, currentSaleID)
		http.Error(w, "Code is from a different sale or sale has ended", http.StatusBadRequest)
		return
	}

	// Execute the Redis Lua script for atomic purchase operations.
	// This script handles the critical logic: decrementing available items,
	// incrementing user purchase counts, and deleting the used checkout code.
	success, message, err := h.RedisManager.ExecutePurchaseScript(saleID, userID, code)
	if err != nil {
		log.Printf("ERROR: Failed to execute Redis purchase script for code %s (user %s, item %s, sale %d): %v", code, userID, itemID, saleID, err)
		http.Error(w, "Failed to execute purchase script", http.StatusInternalServerError)
		return
	}

	// If the Redis script indicates failure (e.g., sale sold out, user limit reached),
	// return the appropriate error message to the client.
	if !success {
		log.Printf("INFO: Purchase denied for user %s, item %s, code %s: %s", userID, itemID, code, message)
		http.Error(w, message, http.StatusBadRequest)
		return
	}

	// Enqueue a `PurchaseJob` to a background worker.
	// This job will asynchronously persist the successful purchase details
	// to PostgreSQL and mark the checkout attempt as used.
	h.PurchaseChan <- models.PurchaseJob{
		UserID: userID,
		SaleID: saleID,
		ItemID: itemID,
		Code:   code,
	}
	log.Printf("INFO: Purchase job enqueued for user %s, item %s, code %s.", userID, itemID, code)

	// Generate synthetic item details for the response, as per contest rules.
	item := h.SaleService.GenerateItem(itemID)

	// Prepare and send the successful purchase response.
	response := models.PurchaseResponse{
		Success:   true,
		Message:   "Item Purchased. ",
		ItemID:    item.ID,
		ItemName:  item.Name,
		ItemImage: item.Image,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
