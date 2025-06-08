package service

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/user/not-contest/internal/db"
)

// ReconciliationService is responsible for periodically reconciling data discrepancies
// between Redis and PostgreSQL to ensure data consistency in the flash-sale system.
type ReconciliationService struct {
	// RedisManager provides methods to interact with Redis for current sale state.
	RedisManager *db.RedisManager
	// PostgresManager provides methods to interact with PostgreSQL for persistent data.
	PostgresManager *db.PostgresManager
}

// NewReconciliationService creates and returns a new instance of ReconciliationService.
// It requires both a RedisManager and a PostgresManager to perform its duties.
func NewReconciliationService(
	redisManager *db.RedisManager,
	postgresManager *db.PostgresManager,
) *ReconciliationService {
	return &ReconciliationService{
		RedisManager:    redisManager,
		PostgresManager: postgresManager,
	}
}

// StartReconciliation initiates the background reconciliation process.
// It runs `reconcileRedisWithDB` in a new goroutine to avoid blocking the main execution flow.
func (s *ReconciliationService) StartReconciliation() {
	go s.reconcileRedisWithDB()
}

// reconcileRedisWithDB is a long-running goroutine that periodically performs
// reconciliation checks between Redis and PostgreSQL. It uses a ticker to
// trigger reconciliation at fixed intervals (e.g., every 10 seconds).
//
// The reconciliation process involves:
// 1. Getting the current active sale ID from PostgreSQL.
// 2. Reconciling checkout attempts: ensuring Redis codes match PostgreSQL records.
// 3. Reconciling sale totals: aligning item counts between Redis and PostgreSQL.
// 4. Reconciling codes: ensuring codes in Redis are reflected in PostgreSQL.
// 5. Reconciling user purchases: synchronizing user purchase counts.
func (s *ReconciliationService) reconcileRedisWithDB() {
	// Create a ticker that fires every 10 seconds.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop() // Ensure the ticker is stopped when the function exits.

	// Loop indefinitely, executing on each tick.
	for range ticker.C {
		log.Println("Starting reconciliation...")

		// Get the current active sale ID from the persistent database (PostgreSQL).
		saleID, err := s.PostgresManager.GetCurrentSaleID()
		if err != nil {
			log.Printf("ERROR: Reconciliation failed to get current sale ID: %v. Skipping this cycle.", err)
			continue // Skip this cycle if we can't determine the current sale.
		}

		// Reconcile checkout attempts.
		if err := s.reconcileCheckouts(saleID); err != nil {
			log.Printf("ERROR: Failed to reconcile checkouts: %v", err)
		}

		// Reconcile the overall sale total (available items).
		if err := s.reconcileSaleTotal(saleID); err != nil {
			log.Printf("ERROR: Failed to reconcile sale total: %v", err)
		}

		// Reconcile temporary checkout codes.
		if err := s.reconcileCodes(); err != nil {
			log.Printf("ERROR: Failed to reconcile codes: %v", err)
		}

		// Reconcile individual user purchase counts.
		if err := s.reconcilePurchases(saleID); err != nil {
			log.Printf("ERROR: Failed to reconcile purchases: %v", err)
		}

		log.Println("Reconciliation complete.")
	}
}

// reconcileCheckouts ensures that unused checkout attempts recorded in PostgreSQL
// have corresponding, valid codes in Redis. If a code is missing in Redis, it's re-added.
// If a code exists but has mismatched data, it's corrected in Redis.
func (s *ReconciliationService) reconcileCheckouts(saleID int) error {
	// Retrieve all unused checkout attempts from PostgreSQL.
	attempts, err := s.PostgresManager.GetUnusedCheckoutAttempts()
	if err != nil {
		return fmt.Errorf("failed to get unused checkout attempts from DB: %w", err)
	}

	for _, attempt := range attempts {
		// Only reconcile attempts belonging to the current active sale.
		if attempt.SaleID != saleID {
			continue
		}

		// Check if the checkout code exists and is valid in Redis.
		codeValue, err := s.RedisManager.GetCodeValue(attempt.Code)
		if err != nil {
			// If the code doesn't exist in Redis (e.g., expired or lost), re-store it.
			log.Printf("INFO: Code '%s' for user %s, item %s, sale %d not found in Redis. Re-adding.", attempt.Code, attempt.UserID, attempt.ItemID, attempt.SaleID)
			storeErr := s.RedisManager.StoreCode(attempt.Code, attempt.UserID, attempt.ItemID, attempt.SaleID)
			if storeErr != nil {
				log.Printf("ERROR: Failed to re-add code '%s' to Redis: %v", attempt.Code, storeErr)
			}
		} else {
			// If the code exists in Redis, parse its value and compare with DB record.
			parts := strings.Split(codeValue, ":")
			if len(parts) != 3 {
				log.Printf("WARNING: Invalid code value format in Redis for code '%s': %s. Skipping.", attempt.Code, codeValue)
				continue
			}

			userID := parts[0]
			itemID := parts[1]
			codeSaleID, parseErr := strconv.Atoi(parts[2])
			if parseErr != nil {
				log.Printf("WARNING: Failed to parse sale ID from Redis code value '%s' for code '%s': %v. Skipping.", parts[2], attempt.Code, parseErr)
				continue
			}

			// If Redis data doesn't match PostgreSQL data, update Redis.
			if userID != attempt.UserID || itemID != attempt.ItemID || codeSaleID != attempt.SaleID {
				log.Printf("WARNING: Code '%s' has mismatched values between Redis (%s) and DB (%s:%s:%d). Updating Redis.",
					attempt.Code, codeValue, attempt.UserID, attempt.ItemID, attempt.SaleID)
				updateErr := s.RedisManager.StoreCode(attempt.Code, attempt.UserID, attempt.ItemID, attempt.SaleID)
				if updateErr != nil {
					log.Printf("ERROR: Failed to update code '%s' in Redis with correct values: %v", attempt.Code, updateErr)
				}
			}
		}
	}

	return nil
}

// reconcileSaleTotal ensures that the total count of items sold for the current sale
// is consistent between Redis and PostgreSQL. Redis is updated to match the database.
func (s *ReconciliationService) reconcileSaleTotal(saleID int) error {
	// Get the total number of items sold for the current sale from PostgreSQL.
	dbTotal, err := s.PostgresManager.GetSaleTotal(saleID)
	if err != nil {
		return fmt.Errorf("failed to get sale total from DB for sale ID %d: %w", saleID, err)
	}

	// Get the total number of items sold for the current sale from Redis.
	redisTotal, err := s.RedisManager.GetSaleTotal(saleID)
	if err != nil {
		// If the sale total key doesn't exist in Redis, set it to the DB value.
		log.Printf("INFO: Sale total for sale %d not found in Redis. Setting it to DB value (%d).", saleID, dbTotal)
		return s.RedisManager.SetSaleTotal(saleID, dbTotal)
	}

	// If the Redis and DB totals don't match, update Redis to reflect the DB's truth.
	if redisTotal != dbTotal {
		log.Printf("WARNING: Sale total mismatch for sale %d: Redis=%d, DB=%d. Updating Redis to DB value.", saleID, redisTotal, dbTotal)
		return s.RedisManager.SetSaleTotal(saleID, dbTotal)
	}

	return nil
}

// reconcileCodes identifies codes present in Redis that are not recorded in PostgreSQL
// as checkout attempts and creates corresponding records in the database.
func (s *ReconciliationService) reconcileCodes() error {
	// Get all checkout code keys currently stored in Redis.
	redisCodeKeys, err := s.RedisManager.GetAllCodes()
	if err != nil {
		return fmt.Errorf("failed to get all codes from Redis: %w", err)
	}

	// Get all checkout codes already recorded in PostgreSQL.
	dbCodes, err := s.PostgresManager.GetCheckoutCodes()
	if err != nil {
		return fmt.Errorf("failed to get checkout codes from DB: %w", err)
	}

	// Create a quick lookup map for database codes.
	dbCodeMap := make(map[string]bool)
	for _, code := range dbCodes {
		dbCodeMap[code] = true
	}

	// Iterate through Redis codes to find those missing in the database.
	for _, redisCodeKey := range redisCodeKeys {
		// Extract the code from the key
		code := strings.TrimPrefix(redisCodeKey, "code:")

		if !dbCodeMap[code] {
			// This code exists in Redis but not in PostgreSQL.
			log.Printf("WARNING: Code '%s' exists in Redis but not in the database. Creating checkout attempt in DB.", code)

			// Retrieve the detailed value associated with the code from Redis.
			codeValue, getErr := s.RedisManager.GetCodeValue(code)
			if getErr != nil {
				log.Printf("ERROR: Failed to get code value from Redis for code '%s' during reconciliation: %v. Skipping creation.", code, getErr)
				continue
			}

			// Parse the code value (userID:itemID:saleID) to get the necessary details.
			parts := strings.Split(codeValue, ":")
			if len(parts) != 3 {
				log.Printf("WARNING: Invalid code value format in Redis for code '%s': %s. Cannot create DB record. Skipping.", code, codeValue)
				continue
			}

			userID := parts[0]
			itemID := parts[1]
			saleID, parseErr := strconv.Atoi(parts[2])
			if parseErr != nil {
				log.Printf("WARNING: Failed to parse sale ID from Redis code value '%s' for code '%s': %v. Skipping creation.", parts[2], code, parseErr)
				continue
			}

			// Create a new checkout attempt record in PostgreSQL.
			_, createErr := s.PostgresManager.CreateCheckoutAttempt(userID, saleID, itemID, code)
			if createErr != nil {
				log.Printf("ERROR: Failed to create checkout attempt in DB for code '%s' (user %s, item %s, sale %d): %v", code, userID, itemID, saleID, createErr)
			} else {
				log.Printf("INFO: Successfully created missing checkout attempt in DB for code '%s'.", code)
			}
		}
	}
	return nil
}

// reconcilePurchases ensures that the number of purchases recorded for each user
// in Redis matches the number of purchases recorded in PostgreSQL. If there's
// a discrepancy (Redis has more purchases than DB), it corrects the DB by
// creating missing purchase records and marking corresponding checkout attempts as used.
func (s *ReconciliationService) reconcilePurchases(saleID int) error {
	// Get all user purchase keys from Redis for the current sale.
	userPurchaseKeys, err := s.RedisManager.GetAllUserPurchaseKeys(saleID)
	if err != nil {
		return fmt.Errorf("failed to get all user purchase keys from Redis for sale %d: %w", saleID, err)
	}

	// For each user, check if the purchase count matches the database
	for _, key := range userPurchaseKeys {
		// Extract the user ID from the key (user:<userID>:sale:<saleID>:purchases)
		parts := strings.Split(key, ":")
		if len(parts) != 5 || parts[0] != "user" || parts[2] != "sale" || parts[4] != "purchases" {
			log.Printf("WARNING: Invalid user purchase key format encountered in Redis: %s. Skipping.", key)
			continue
		}

		userID := parts[1]

		// Get the user's purchase count from Redis.
		redisPurchases, err := s.RedisManager.GetUserPurchases(saleID, userID)
		if err != nil {
			log.Printf("ERROR: Failed to get user purchases from Redis for user %s, sale %d: %v. Skipping.", userID, saleID, err)
			continue
		}

		// Get the user's purchase count from PostgreSQL.
		dbPurchases, err := s.PostgresManager.GetUserPurchases(userID, saleID)
		if err != nil {
			log.Printf("ERROR: Failed to get user purchases from database for user %s, sale %d: %v. Skipping.", userID, saleID, err)
			continue
		}

		// If Redis has more purchases than PostgreSQL, reconcile the database.
		if redisPurchases > dbPurchases {
			log.Printf("WARNING: User %s purchase count mismatch for sale %d: Redis has %d, DB has %d. Reconciling DB.", userID, saleID, redisPurchases, dbPurchases)

			// Calculate the number of purchases that need to be added to the database.
			purchasesToAdd := redisPurchases - dbPurchases
			if purchasesToAdd <= 0 {
				continue // Should not happen if redisPurchases > dbPurchases, but as a safeguard.
			}

			// Begin a database transaction to ensure atomicity of reconciliation changes.
			tx, err := s.PostgresManager.BeginTransaction()
			if err != nil {
				log.Printf("ERROR: Failed to begin transaction for purchase reconciliation (user %s, sale %d): %v. Skipping.", userID, saleID, err)
				continue
			}
			defer tx.Rollback() // Rollback in case of errors, unless explicitly committed.

			// Attempt to find existing unused checkout attempts that can be converted to purchases.
			// This query tries to find `purchasesToAdd` number of unused checkouts for the user and sale.
			rows, queryErr := tx.Query(
				"SELECT id, item_id, code FROM checkout_attempts WHERE user_id = $1 AND sale_id = $2 AND used = FALSE ORDER BY created_at ASC LIMIT $3",
				userID, saleID, purchasesToAdd,
			)
			if queryErr != nil {
				tx.Rollback()
				log.Printf("ERROR: Failed to query unused checkout attempts for user %s, sale %d: %v", userID, saleID, queryErr)
				continue
			}

			// Mark checkout attempts as used and create purchase records
			var checkoutIDs []int
			var itemIDs []string
			// var codes []string

			for rows.Next() {
				var id int
				var itemID, code string
				if scanErr := rows.Scan(&id, &itemID, &code); scanErr != nil {
					log.Printf("ERROR: Failed to scan checkout attempt row for user %s, sale %d: %v. Continuing with next.", userID, saleID, scanErr)
					continue
				}
				checkoutIDs = append(checkoutIDs, id)
				itemIDs = append(itemIDs, itemID)
				// codes = append(codes, code)
			}
			rows.Close()

			// If there are still missing purchases even after utilizing existing unused checkouts,
			// create new "reconciled" checkout attempts and purchases. This handles cases where
			// Redis increments happened but no initial checkout attempt was recorded in DB.
			for i := len(checkoutIDs); i < purchasesToAdd; i++ {
				// Generate a placeholder item ID and code for the reconciled purchase.
				// This might indicate a data anomaly where a purchase occurred in Redis
				// without a corresponding checkout attempt in the DB.
				syntheticItemID := fmt.Sprintf("reconciled-item-%s-%d-%d", userID, saleID, time.Now().UnixNano())
				syntheticCode := fmt.Sprintf("reconciled-code-%s-%d-%d", userID, saleID, time.Now().UnixNano())

				// Insert a new checkout attempt marked as 'used' directly.
				var newCheckoutID int
				insertErr := tx.QueryRow(
					"INSERT INTO checkout_attempts (user_id, sale_id, item_id, code, created_at, used) VALUES ($1, $2, $3, $4, NOW(), TRUE) RETURNING id",
					userID, saleID, syntheticItemID, syntheticCode,
				).Scan(&newCheckoutID)
				if insertErr != nil {
					log.Printf("ERROR: Failed to create synthetic checkout attempt for user %s, sale %d: %v. Continuing.", userID, saleID, insertErr)
					continue
				}

				checkoutIDs = append(checkoutIDs, newCheckoutID)
				itemIDs = append(itemIDs, syntheticItemID) // Add the synthetic item ID to the slice
				// codes = append(codes, syntheticCode)       // Add the synthetic code to the slice
			}

			// Now, for each identified or created checkout attempt, mark it as used
			// and create a corresponding purchase record.
			for i, id := range checkoutIDs {
				// Mark the checkout attempt as used in the database.
				_, execErr := tx.Exec("UPDATE checkout_attempts SET used = TRUE WHERE id = $1", id)
				if execErr != nil {
					log.Printf("ERROR: Failed to mark checkout attempt %d as used for user %s, sale %d: %v. Continuing.", id, userID, saleID, execErr)
					continue
				}

				// Create a purchase record for this item.
				_, execErr = tx.Exec(
					"INSERT INTO purchases (user_id, sale_id, item_id, checkout_id, created_at) VALUES ($1, $2, $3, $4, NOW())",
					userID, saleID, itemIDs[i], id, // Use the itemID from the slice
				)
				if execErr != nil {
					log.Printf("ERROR: Failed to create purchase record for checkout %d (user %s, item %s, sale %d): %v. Continuing.", id, userID, itemIDs[i], saleID, execErr)
					continue
				}
				log.Printf("INFO: Reconciled purchase created for user %s, item %s, checkout %d.", userID, itemIDs[i], id)
			}

			// Commit the transaction if all operations within it were successful.
			if commitErr := tx.Commit(); commitErr != nil {
				log.Printf("ERROR: Failed to commit transaction for purchase reconciliation (user %s, sale %d): %v. Rolling back.", userID, saleID, commitErr)
				// Defer already handles rollback if commit fails.
				return fmt.Errorf("transaction commit failed for user %s, sale %d: %w", userID, saleID, commitErr)
			}
			log.Printf("INFO: Successfully reconciled %d purchases for user %s, sale %d.", purchasesToAdd, userID, saleID)
		}
	}

	return nil
}
