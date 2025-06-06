package db

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

// PostgresManager handles PostgreSQL operations
type PostgresManager struct {
	DB *sql.DB
}

// NewPostgresManager creates a new PostgresManager
func NewPostgresManager(db *sql.DB) *PostgresManager {
	return &PostgresManager{
		DB: db,
	}
}

// InitializeTables creates the necessary tables if they don't exist
func (pm *PostgresManager) InitializeTables() error {
	// Create sales table
	_, err := pm.DB.Exec(`
		CREATE TABLE IF NOT EXISTS sales (
			id SERIAL PRIMARY KEY,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			ended_at TIMESTAMP
		)
	`)
	if err != nil {
		log.Printf("Failed to create sales table: %v", err)
		return err
	}

	// Create checkout_attempts table
	_, err = pm.DB.Exec(`
		CREATE TABLE IF NOT EXISTS checkout_attempts (
			id SERIAL PRIMARY KEY,
			user_id TEXT NOT NULL,
			sale_id INTEGER NOT NULL REFERENCES sales(id),
			item_id TEXT NOT NULL,
			code TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			used BOOLEAN NOT NULL DEFAULT FALSE
		)
	`)
	if err != nil {
		log.Printf("Failed to create checkout_attempts table: %v", err)
		return err
	}

	// Create purchases table
	_, err = pm.DB.Exec(`
		CREATE TABLE IF NOT EXISTS purchases (
			id SERIAL PRIMARY KEY,
			user_id TEXT NOT NULL,
			sale_id INTEGER NOT NULL REFERENCES sales(id),
			item_id TEXT NOT NULL,
			checkout_id INTEGER REFERENCES checkout_attempts(id),
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		log.Printf("Failed to create purchases table: %v", err)
		return err
	}

	return nil
}

// EnsureSaleExists ensures that a sale with the given ID exists
func (pm *PostgresManager) EnsureSaleExists(saleID int) error {
	// Check if the sale exists
	var exists bool
	err := pm.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM sales WHERE id = $1)", saleID).Scan(&exists)
	if err != nil {
		log.Printf("Failed to check if sale exists: %v", err)
		return err
	}

	// If the sale doesn't exist, create it
	if !exists {
		_, err = pm.DB.Exec("INSERT INTO sales (id, created_at) VALUES ($1, NOW())", saleID)
		if err != nil {
			log.Printf("Failed to create sale: %v", err)
			return err
		}
	}

	return nil
}

// CreateSale creates a new sale
func (pm *PostgresManager) CreateSale() (int, error) {
	var saleID int
	err := pm.DB.QueryRow("INSERT INTO sales (created_at) VALUES (NOW()) RETURNING id").Scan(&saleID)
	if err != nil {
		log.Printf("Failed to create sale: %v", err)
		return 0, err
	}

	return saleID, nil
}

// GetCurrentSaleID gets the ID of the current sale
func (pm *PostgresManager) GetCurrentSaleID() (int, error) {
	var saleID int
	err := pm.DB.QueryRow(
		"SELECT id FROM sales WHERE ended_at IS NULL ORDER BY created_at DESC LIMIT 1",
	).Scan(&saleID)

	if err != nil {
		if err == sql.ErrNoRows {
			// No active sale, create one
			return pm.CreateSale()
		}
		log.Printf("Failed to get current sale ID: %v", err)
		return 0, err
	}

	return saleID, nil
}

// EndSale marks a sale as ended
func (pm *PostgresManager) EndSale(saleID int) error {
	_, err := pm.DB.Exec("UPDATE sales SET ended_at = NOW() WHERE id = $1", saleID)
	if err != nil {
		log.Printf("Failed to end sale: %v", err)
		return err
	}

	return nil
}

// CreateCheckoutAttempt creates a new checkout attempt
func (pm *PostgresManager) CreateCheckoutAttempt(userID string, saleID int, itemID, code string) (int, error) {
	var attemptID int
	err := pm.DB.QueryRow(
		"INSERT INTO checkout_attempts (user_id, sale_id, item_id, code, created_at) VALUES ($1, $2, $3, $4, NOW()) RETURNING id",
		userID, saleID, itemID, code,
	).Scan(&attemptID)

	if err != nil {
		log.Printf("Failed to create checkout attempt: %v", err)
		return 0, err
	}

	return attemptID, nil
}

// MarkCheckoutAttemptAsUsed marks a checkout attempt as used
func (pm *PostgresManager) MarkCheckoutAttemptAsUsed(code string) error {
	_, err := pm.DB.Exec("UPDATE checkout_attempts SET used = TRUE WHERE code = $1", code)
	if err != nil {
		log.Printf("Failed to mark checkout attempt as used: %v", err)
		return err
	}

	return nil
}

// GetCheckoutAttempt gets a checkout attempt by code
func (pm *PostgresManager) GetCheckoutAttempt(code string) (int, string, int, string, error) {
	var id int
	var userID string
	var saleID int
	var itemID string

	err := pm.DB.QueryRow(
		"SELECT id, user_id, sale_id, item_id FROM checkout_attempts WHERE code = $1 AND used = FALSE",
		code,
	).Scan(&id, &userID, &saleID, &itemID)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, "", 0, "", fmt.Errorf("checkout attempt not found or already used")
		}
		log.Printf("Failed to get checkout attempt: %v", err)
		return 0, "", 0, "", err
	}

	return id, userID, saleID, itemID, nil
}

// CreatePurchase creates a new purchase
func (pm *PostgresManager) CreatePurchase(userID string, saleID int, itemID string, checkoutID int) (int, error) {
	var purchaseID int
	err := pm.DB.QueryRow(
		"INSERT INTO purchases (user_id, sale_id, item_id, checkout_id, created_at) VALUES ($1, $2, $3, $4, NOW()) RETURNING id",
		userID, saleID, itemID, checkoutID,
	).Scan(&purchaseID)

	if err != nil {
		log.Printf("Failed to create purchase: %v", err)
		return 0, err
	}

	return purchaseID, nil
}

// GetSaleTotal gets the total number of purchases for a sale
func (pm *PostgresManager) GetSaleTotal(saleID int) (int, error) {
	var total int
	err := pm.DB.QueryRow(
		"SELECT COUNT(*) FROM purchases WHERE sale_id = $1",
		saleID,
	).Scan(&total)

	if err != nil {
		log.Printf("Failed to get sale total: %v", err)
		return 0, err
	}

	return total, nil
}

// GetUserPurchases gets the number of purchases for a user in a sale
func (pm *PostgresManager) GetUserPurchases(userID string, saleID int) (int, error) {
	var total int
	err := pm.DB.QueryRow(
		"SELECT COUNT(*) FROM purchases WHERE user_id = $1 AND sale_id = $2",
		userID, saleID,
	).Scan(&total)

	if err != nil {
		log.Printf("Failed to get user purchases: %v", err)
		return 0, err
	}

	return total, nil
}

// GetUnusedCheckoutAttempts gets all unused checkout attempts
func (pm *PostgresManager) GetUnusedCheckoutAttempts() ([]struct {
	ID     int
	UserID string
	SaleID int
	ItemID string
	Code   string
}, error) {
	rows, err := pm.DB.Query(
		"SELECT id, user_id, sale_id, item_id, code FROM checkout_attempts WHERE used = FALSE AND created_at > $1",
		time.Now().Add(-time.Hour), // Only get attempts from the last hour
	)
	if err != nil {
		log.Printf("Failed to get unused checkout attempts: %v", err)
		return nil, err
	}
	defer rows.Close()

	var attempts []struct {
		ID     int
		UserID string
		SaleID int
		ItemID string
		Code   string
	}

	for rows.Next() {
		var attempt struct {
			ID     int
			UserID string
			SaleID int
			ItemID string
			Code   string
		}

		err := rows.Scan(&attempt.ID, &attempt.UserID, &attempt.SaleID, &attempt.ItemID, &attempt.Code)
		if err != nil {
			log.Printf("Failed to scan checkout attempt: %v", err)
			return nil, err
		}

		attempts = append(attempts, attempt)
	}

	return attempts, nil
}

// GetCheckoutCodes gets all checkout codes
func (pm *PostgresManager) GetCheckoutCodes() ([]string, error) {
	rows, err := pm.DB.Query(
		"SELECT code FROM checkout_attempts WHERE used = FALSE AND created_at > $1",
		time.Now().Add(-time.Hour), // Only get attempts from the last hour
	)
	if err != nil {
		log.Printf("Failed to get checkout codes: %v", err)
		return nil, err
	}
	defer rows.Close()

	var codes []string

	for rows.Next() {
		var code string

		err := rows.Scan(&code)
		if err != nil {
			log.Printf("Failed to scan checkout code: %v", err)
			return nil, err
		}

		codes = append(codes, code)
	}

	return codes, nil
}

// BeginTransaction begins a new transaction
func (pm *PostgresManager) BeginTransaction() (*sql.Tx, error) {
	return pm.DB.Begin()
}
