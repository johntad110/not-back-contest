package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
	"github.com/user/not-contest/internal/service"
	"github.com/user/not-contest/internal/utils"
)

// CheckoutHandler handles HTTP requests for the /checkout endpoint.
// It orchestrates the initial phase of a flash sale transaction,
// validating limits using Redis, generating a unique checkout code,
// storing it temporarily in Redis, and enqueuing a job for background persistence.
type CheckoutHandler struct {
	// RedisManager provides methods for interacting with Redis,
	// particularly for atomic operations and temporary data storage.
	RedisManager *db.RedisManager
	// PostgresManager provides methods for interacting with PostgreSQL.
	// While not directly used for immediate persistence in this handler,
	// it's a dependency for the SaleService.
	PostgresManager *db.PostgresManager
	// SaleService provides business logic related to sale management,
	// such as retrieving the current sale ID.
	SaleService *service.SaleService
	// CheckoutChan is a channel used to send asynchronous checkout jobs
	// to a background worker for persistent storage in PostgreSQL.
	CheckoutChan chan<- models.CheckoutJob
}

// NewCheckoutHandler creates and returns a new instance of CheckoutHandler.
// It takes necessary dependencies such as RedisManager, PostgresManager,
// SaleService, and a channel for checkout jobs.
func NewCheckoutHandler(
	redisManager *db.RedisManager,
	postgresManager *db.PostgresManager,
	saleService *service.SaleService,
	checkoutChan chan<- models.CheckoutJob,
) *CheckoutHandler {
	return &CheckoutHandler{
		RedisManager:    redisManager,
		PostgresManager: postgresManager,
		SaleService:     saleService,
		CheckoutChan:    checkoutChan,
	}
}

// Handle processes incoming HTTP requests for the /checkout endpoint.
// It expects a POST request with `user_id` and `id` (item ID) query parameters.
//
// The flow involves:
//  1. Validating the HTTP method and required query parameters.
//  2. Retrieving the current active sale ID.
//  3. Atomically checking sale and user limits using a Redis Lua script.
//  4. If limits are respected, generating a unique checkout code.
//  5. Storing the generated code in Redis with an expiration.
//  6. Enqueuing a `CheckoutJob` to a background worker for asynchronous
//     persistence of the checkout attempt in PostgreSQL.
//  7. Responding to the client with the generated checkout code.
//
// Errors are handled by sending appropriate HTTP status codes and messages.
func (h *CheckoutHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Only allow POST requests for checkout.
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract user_id and item_id from URL query parameters.
	userID := r.URL.Query().Get("user_id")
	itemID := r.URL.Query().Get("id")

	// Validate required parameters.
	if userID == "" || itemID == "" {
		http.Error(w, "User ID and Item ID are required", http.StatusBadRequest)
		return
	}

	// Generate a unique alphanumeric code for this checkout attempt.
	code, err := utils.GenerateCode()
	if err != nil {
		log.Printf("ERROR: Failed to generate unique code for user %s, item %s: %v", userID, itemID, err)
		http.Error(w, "Failed to generate checkout code", http.StatusInternalServerError)
		return
	}

	// Execute the Redis Lua script for atomic checkout validation.
	// This script checks if the overall sale limit or the user's individual
	// purchase limit has been reached. It also atomically increments the user's
	// checkout counter in Redis.
	success, message, saleID, err := h.RedisManager.ExecuteCheckoutScript(userID, itemID, code)
	if err != nil {
		log.Printf("ERROR: Failed to execute Redis checkout script for user %s, item %s, sale %d: %v", userID, itemID, saleID, err)
		http.Error(w, "Failed to process checkout", http.StatusInternalServerError)
		return
	}

	// If the Redis script indicates failure (e.g., limits reached), return the reason.
	if !success {
		log.Printf("INFO: Checkout denied for user %s, item %s, sale %d: %s", userID, itemID, saleID, message)
		http.Error(w, message, http.StatusBadRequest)
		return
	}

	// Store the generated code in Redis with an expiration. This code acts as a
	// temporary token that the user can use to complete the purchase later.
	if err := h.RedisManager.StoreCode(code, userID, itemID, saleID); err != nil {
		log.Printf("ERROR: Failed to store code %s in Redis for user %s, item %s, sale %d: %v", code, userID, itemID, saleID, err)
		http.Error(w, "Failed to store checkout code", http.StatusInternalServerError)
		return
	}

	// Asynchronously persist the checkout attempt to PostgreSQL.
	// This decouples the HTTP request response from the slower database write,
	// improving API throughput. The worker will pick this job from the channel.
	h.CheckoutChan <- models.CheckoutJob{
		UserID: userID,
		SaleID: saleID,
		ItemID: itemID,
		Code:   code,
	}

	// Return the generated code
	response := models.CheckoutResponse{
		Code: code,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
