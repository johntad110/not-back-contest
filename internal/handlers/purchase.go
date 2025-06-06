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

// PurchaseHandler handles purchase requests
type PurchaseHandler struct {
	RedisManager    *db.RedisManager
	PostgresManager *db.PostgresManager
	SaleService     *service.SaleService
	PurchaseChan    chan<- models.PurchaseJob
}

// NewPurchaseHandler creates a new PurchaseHandler
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

// Handle handles purchase requests
func (h *PurchaseHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Ensure it's a POST request
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the code from the URL query parameter
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing code param", http.StatusBadRequest)
		return
	}

	// Get the code value from Redis
	codeValue, err := h.RedisManager.GetCodeValue(code)
	if err != nil {
		log.Printf("Failed to get code value from Redis: %v", err)
		http.Error(w, "Invalid or expired code", http.StatusBadRequest)
		return
	}

	// Parse the code value (userID:itemID:saleID)
	parts := strings.Split(codeValue, ":")
	if len(parts) != 3 {
		log.Printf("Invalid code value format: %s", codeValue)
		http.Error(w, "Invalid code format", http.StatusBadRequest)
		return
	}

	userID := parts[0]
	itemID := parts[1]
	saleID, err := strconv.Atoi(parts[2]) // Use strconv.Atoi for parsing integer from string
	if err != nil {
		log.Printf("Failed to parse sale ID from code value: %v", err)
		http.Error(w, "Invalid code format", http.StatusBadRequest)
		return
	}

	// Verify the sale ID
	currentSaleID, err := h.SaleService.GetCurrentSaleID()
	if err != nil {
		log.Printf("Failed to get current sale ID: %v", err)
		http.Error(w, "Failed to get current sale", http.StatusInternalServerError)
		return
	}

	if saleID != currentSaleID {
		http.Error(w, "Code is from a different sale", http.StatusBadRequest)
		return
	}

	// Execute purchase script
	success, message, err := h.RedisManager.ExecutePurchaseScript(saleID, userID, code)
	if err != nil {
		log.Printf("Failed to execute purchase script: %v", err)
		http.Error(w, "Failed to execute purchase script", http.StatusInternalServerError)
		return
	}

	if !success {
		http.Error(w, message, http.StatusBadRequest)
		return
	}

	// Send a job to the background worker
	h.PurchaseChan <- models.PurchaseJob{
		UserID: userID,
		SaleID: saleID,
		ItemID: itemID,
		Code:   code,
	}

	// Generate item details
	item := h.SaleService.GenerateItem(itemID)

	// Return success
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
