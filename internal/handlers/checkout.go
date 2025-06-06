package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
	"github.com/user/not-contest/internal/service"
	"github.com/user/not-contest/internal/utils" // Import the new utils package
)

// CheckoutHandler handles checkout requests
type CheckoutHandler struct {
	RedisManager    *db.RedisManager
	PostgresManager *db.PostgresManager
	SaleService     *service.SaleService
	CheckoutChan    chan<- models.CheckoutJob
}

// NewCheckoutHandler creates a new CheckoutHandler
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

// Handle handles checkout requests
func (h *CheckoutHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Ensure it's a POST request
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	userID := r.URL.Query().Get("user_id")
	itemID := r.URL.Query().Get("id")

	if userID == "" || itemID == "" {
		http.Error(w, "User ID and Item ID are required", http.StatusBadRequest)
		return
	}

	// Get current sale ID
	saleID, err := h.SaleService.GetCurrentSaleID()
	if err != nil {
		log.Printf("Failed to get current sale ID: %v", err)
		http.Error(w, "Failed to get current sale", http.StatusInternalServerError)
		return
	}

	// Ensure sale exists
	if saleID == 0 {
		http.Error(w, "No active sale", http.StatusNotFound)
		return
	}

	// Execute Redis checkout script
	success, message, err := h.RedisManager.ExecuteCheckoutScript(saleID, userID)
	if err != nil {
		log.Printf("Failed to execute checkout script: %v", err)
		http.Error(w, "Failed to process checkout", http.StatusInternalServerError)
		return
	}

	if !success {
		http.Error(w, message, http.StatusBadRequest)
		return
	}

	// Generate a unique code
	code, err := utils.GenerateCode()
	if err != nil {
		log.Printf("Failed to generate code: %v", err)
		http.Error(w, "Failed to generate code", http.StatusInternalServerError)
		return
	}

	// Store code in Redis with expiration
	if err := h.RedisManager.StoreCode(code, userID, itemID, saleID); err != nil {
		log.Printf("Failed to store code in Redis: %v", err)
		http.Error(w, "Failed to store checkout code", http.StatusInternalServerError)
		return
	}

	// Set expiration on user's checkouts key
	if err := h.RedisManager.SetUserCheckouts(saleID, userID, 1); err != nil {
		log.Printf("Failed to set user checkouts expiration: %v", err)
		http.Error(w, "Failed to set user checkouts expiration", http.StatusInternalServerError)
		return
	}

	// Send a job to the background worker
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
