package service

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/user/not-contest/internal/db"
)

// ReconciliationService handles reconciliation between Redis and PostgreSQL
type ReconciliationService struct {
	RedisManager    *db.RedisManager
	PostgresManager *db.PostgresManager
}

// NewReconciliationService creates a new ReconciliationService
func NewReconciliationService(
	redisManager *db.RedisManager,
	postgresManager *db.PostgresManager,
) *ReconciliationService {
	return &ReconciliationService{
		RedisManager:    redisManager,
		PostgresManager: postgresManager,
	}
}

// StartReconciliation starts the reconciliation process
func (s *ReconciliationService) StartReconciliation() {
	go s.reconcileRedisWithDB()
}

// reconcileRedisWithDB periodically reconciles Redis with the database
func (s *ReconciliationService) reconcileRedisWithDB() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		log.Println("Starting reconciliation")

		// Get current sale ID
		saleID, err := s.PostgresManager.GetCurrentSaleID()
		if err != nil {
			log.Printf("Failed to get current sale ID: %v", err)
			continue
		}

		// Reconcile checkouts
		if err := s.reconcileCheckouts(saleID); err != nil {
			log.Printf("Failed to reconcile checkouts: %v", err)
		}

		// Reconcile sale total
		if err := s.reconcileSaleTotal(saleID); err != nil {
			log.Printf("Failed to reconcile sale total: %v", err)
		}

		// Reconcile codes
		if err := s.reconcileCodes(); err != nil {
			log.Printf("Failed to reconcile codes: %v", err)
		}

		// Reconcile purchases
		if err := s.reconcilePurchases(saleID); err != nil {
			log.Printf("Failed to reconcile purchases: %v", err)
		}

		log.Println("Reconciliation complete")
	}
}

// reconcileCheckouts reconciles checkout attempts between Redis and the database
func (s *ReconciliationService) reconcileCheckouts(saleID int) error {
	// Get all unused checkout attempts from the database
	attempts, err := s.PostgresManager.GetUnusedCheckoutAttempts()
	if err != nil {
		return err
	}

	// For each attempt, check if the code exists in Redis
	for _, attempt := range attempts {
		// Skip attempts from other sales
		if attempt.SaleID != saleID {
			continue
		}

		// Check if the code exists in Redis
		codeValue, err := s.RedisManager.GetCodeValue(attempt.Code)
		if err != nil {
			// Code doesn't exist in Redis, add it back
			log.Printf("Adding code %s back to Redis", attempt.Code)
			err = s.RedisManager.StoreCode(attempt.Code, attempt.UserID, attempt.ItemID, attempt.SaleID)
			if err != nil {
				log.Printf("Failed to add code back to Redis: %v", err)
			}
		} else {
			// Code exists in Redis, check if it matches the database
			parts := strings.Split(codeValue, ":")
			if len(parts) != 3 {
				log.Printf("Invalid code value format: %s", codeValue)
				continue
			}

			userID := parts[0]
			itemID := parts[1]
			codeSaleID, err := strconv.Atoi(parts[2])
			if err != nil {
				log.Printf("Failed to parse sale ID from code value: %v", err)
				continue
			}

			if userID != attempt.UserID || itemID != attempt.ItemID || codeSaleID != attempt.SaleID {
				log.Printf("Code %s has mismatched values in Redis and database", attempt.Code)
				// Update Redis with the correct values
				err = s.RedisManager.StoreCode(attempt.Code, attempt.UserID, attempt.ItemID, attempt.SaleID)
				if err != nil {
					log.Printf("Failed to update code in Redis: %v", err)
				}
			}
		}
	}

	return nil
}

// reconcileSaleTotal reconciles the sale total between Redis and the database
func (s *ReconciliationService) reconcileSaleTotal(saleID int) error {
	// Get the sale total from the database
	dbTotal, err := s.PostgresManager.GetSaleTotal(saleID)
	if err != nil {
		return err
	}

	// Get the sale total from Redis
	redisTotal, err := s.RedisManager.GetSaleTotal(saleID)
	if err != nil {
		// Sale total doesn't exist in Redis, set it
		log.Printf("Setting sale total in Redis to %d", dbTotal)
		return s.RedisManager.SetSaleTotal(saleID, dbTotal)
	}

	// If the totals don't match, update Redis
	if redisTotal != dbTotal {
		log.Printf("Sale total mismatch: Redis=%d, DB=%d", redisTotal, dbTotal)
		return s.RedisManager.SetSaleTotal(saleID, dbTotal)
	}

	return nil
}

// reconcileCodes reconciles codes between Redis and the database
func (s *ReconciliationService) reconcileCodes() error {
	// Get all codes from Redis
	redisCodes, err := s.RedisManager.GetAllCodes()
	if err != nil {
		return err
	}

	// Get all codes from the database
	dbCodes, err := s.PostgresManager.GetCheckoutCodes()
	if err != nil {
		return err
	}

	// Create a map of database codes for quick lookup
	dbCodeMap := make(map[string]bool)
	for _, code := range dbCodes {
		dbCodeMap[code] = true
	}

	// Check for codes in Redis that are not in the database
	for _, redisCodeKey := range redisCodes {
		// Extract the code from the key (code:UUID)
		code := strings.TrimPrefix(redisCodeKey, "code:")

		if !dbCodeMap[code] {
			// Code exists in Redis but not in the database
			log.Printf("Code %s exists in Redis but not in the database", code)

			// Get the code value from Redis
			codeValue, err := s.RedisManager.GetCodeValue(code)
			if err != nil {
				log.Printf("Failed to get code value from Redis: %v", err)
				continue
			}

			// Parse the code value (userID:itemID:saleID)
			parts := strings.Split(codeValue, ":")
			if len(parts) != 3 {
				log.Printf("Invalid code value format: %s", codeValue)
				continue
			}

			userID := parts[0]
			itemID := parts[1]
			saleID, err := strconv.Atoi(parts[2])
			if err != nil {
				log.Printf("Failed to parse sale ID from code value: %v", err)
				continue
			}

			// Create a checkout attempt in the database
			_, err = s.PostgresManager.CreateCheckoutAttempt(userID, saleID, itemID, code)
			if err != nil {
				log.Printf("Failed to create checkout attempt: %v", err)
			}
		}
	}

	return nil
}

// reconcilePurchases reconciles purchases between Redis and the database
func (s *ReconciliationService) reconcilePurchases(saleID int) error {
	// Get all user purchase keys from Redis
	userPurchaseKeys, err := s.RedisManager.GetAllUserPurchaseKeys(saleID)
	if err != nil {
		return err
	}

	// For each user, check if the purchase count matches the database
	for _, key := range userPurchaseKeys {
		// Extract the user ID from the key (user:<userID>:sale:<saleID>:purchases)
		parts := strings.Split(key, ":")
		if len(parts) != 5 {
			log.Printf("Invalid user purchase key format: %s", key)
			continue
		}

		userID := parts[1]

		// Get the purchase count from Redis
		redisPurchases, err := s.RedisManager.GetUserPurchases(saleID, userID)
		if err != nil {
			log.Printf("Failed to get user purchases from Redis: %v", err)
			continue
		}

		// Get the purchase count from the database
		dbPurchases, err := s.PostgresManager.GetUserPurchases(userID, saleID)
		if err != nil {
			log.Printf("Failed to get user purchases from database: %v", err)
			continue
		}

		// If the counts don't match, reconcile
		if redisPurchases != dbPurchases {
			log.Printf("User %s purchase count mismatch: Redis=%d, DB=%d", userID, redisPurchases, dbPurchases)

			// Begin a transaction
			tx, err := s.PostgresManager.BeginTransaction()
			if err != nil {
				log.Printf("Failed to begin transaction: %v", err)
				continue
			}

			// Get unused checkout attempts for this user and sale
			rows, queryErr := tx.Query(
				"SELECT id, item_id, code FROM checkout_attempts WHERE user_id = $1 AND sale_id = $2 AND used = FALSE LIMIT $3",
				userID, saleID, redisPurchases-dbPurchases,
			)
			if queryErr != nil {
				tx.Rollback()
				log.Printf("Failed to get unused checkout attempts: %v", queryErr)
				continue
			}

			// Mark checkout attempts as used and create purchase records
			var checkoutIDs []int
			var itemIDs []string
			var codes []string

			for rows.Next() {
				var id int
				var itemID, code string
				if scanErr := rows.Scan(&id, &itemID, &code); scanErr != nil {
					log.Printf("Failed to scan checkout attempt: %v", scanErr)
					continue
				}
				checkoutIDs = append(checkoutIDs, id)
				itemIDs = append(itemIDs, itemID)
				codes = append(codes, code)
			}
			rows.Close()

			// If we don't have enough checkout attempts, create new ones
			for i := len(checkoutIDs); i < redisPurchases-dbPurchases; i++ {
				// Generate a random item ID
				itemID := fmt.Sprintf("reconciled-item-%d", i)
				code := fmt.Sprintf("reconciled-code-%s-%d-%d", userID, saleID, i)

				// Create a checkout attempt
				var id int
				insertErr := tx.QueryRow(
					"INSERT INTO checkout_attempts (user_id, sale_id, item_id, code, created_at, used) VALUES ($1, $2, $3, $4, NOW(), TRUE) RETURNING id",
					userID, saleID, itemID, code,
				).Scan(&id)
				if insertErr != nil {
					log.Printf("Failed to create checkout attempt: %v", insertErr)
					continue
				}

				checkoutIDs = append(checkoutIDs, id)
				itemIDs = append(itemIDs, itemID)
			}

			// Mark checkout attempts as used and create purchase records
			for i, id := range checkoutIDs {
				// Mark the checkout attempt as used
				_, err = tx.Exec("UPDATE checkout_attempts SET used = TRUE WHERE id = $1", id)
				if err != nil {
					log.Printf("Failed to mark checkout attempt as used: %v", err)
					continue
				}

				// Create a purchase record
				_, err = tx.Exec(
					"INSERT INTO purchases (user_id, sale_id, item_id, checkout_id, created_at) VALUES ($1, $2, $3, $4, NOW())",
					userID, saleID, itemIDs[i], id,
				)
				if err != nil {
					log.Printf("Failed to create purchase record: %v", err)
					continue
				}
			}

			// Commit the transaction
			if err = tx.Commit(); err != nil {
				log.Printf("Failed to commit transaction: %v", err)
				tx.Rollback()
			}
		}
	}

	return nil
}
