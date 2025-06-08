package workers

import (
	"database/sql" // Import for checking sql.ErrNoRows.
	"fmt"
	"log"
	"time" // Used for implementing retry delays.

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
)

// PurchaseWorker processes asynchronous purchase jobs.
// Its main responsibility is to durably record confirmed purchases
// in the PostgreSQL database and mark the corresponding checkout attempts as used.
type PurchaseWorker struct {
	// Similar to CheckoutWorker, I don't reemmber why I include RedisManager.
	// it might be for future use or legacy.
	RedisManager *db.RedisManager
	// PostgresManager provides methods to interact with PostgreSQL,
	// where purchase records and checkout attempt updates are made.
	PostgresManager *db.PostgresManager
	// PurchaseChan is a receive-only channel from which the worker
	// reads `PurchaseJob`s enqueued by the HTTP handlers.
	PurchaseChan <-chan models.PurchaseJob
}

// NewPurchaseWorker creates and returns a new instance of PurchaseWorker.
// It initializes the worker with the necessary database managers and the job channel.
func NewPurchaseWorker(
	redisManager *db.RedisManager,
	postgresManager *db.PostgresManager,
	purchaseChan <-chan models.PurchaseJob,
) *PurchaseWorker {
	return &PurchaseWorker{
		RedisManager:    redisManager,
		PostgresManager: postgresManager,
		PurchaseChan:    purchaseChan,
	}
}

// Start initiates the purchase worker's processing loop.
// It runs `processPurchaseJobs` in a new goroutine, allowing for concurrent operation.
func (w *PurchaseWorker) Start() {
	go w.processPurchaseJobs()
}

// processPurchaseJobs continuously reads `PurchaseJob`s from the `PurchaseChan`.
// For each job, it attempts to complete the purchase by updating the database.
func (w *PurchaseWorker) processPurchaseJobs() {
	// The `for job := range w.PurchaseChan` loop blocks until a job is available
	// on the channel, and then processes it. This loop runs indefinitely.
	for job := range w.PurchaseChan {
		log.Printf("INFO: Processing purchase job: UserID=%s, SaleID=%d, ItemID=%s, Code=%s",
			job.UserID, job.SaleID, job.ItemID, job.Code)

		// Attempt to process the purchase, including database updates.
		if err := w.processPurchase(job); err != nil {
			log.Printf("ERROR: Failed to process purchase for job %+v: %v. Scheduling retry...", job, err)
			// If processing fails, schedule a retry.
			go w.retryPurchase(job)
		} else {
			log.Printf("INFO: Successfully processed purchase for code %s in DB.", job.Code)
		}
	}
}

// processPurchase handles the core logic for persisting a purchase.
// It ensures that the corresponding checkout attempt is marked as used
// and a new purchase record is created in PostgreSQL, all within a database transaction.
func (w *PurchaseWorker) processPurchase(job models.PurchaseJob) error {
	// First, retrieve the existing checkout attempt details from PostgreSQL
	// using the provided code. This verifies the checkout attempt exists.
	checkoutID, userID, saleID, itemID, err := w.PostgresManager.GetCheckoutAttempt(job.Code)
	if err != nil {
		if err == sql.ErrNoRows {
			// If the checkout attempt is not found, it's a critical error for this job.
			// This could happen if the reconciliation service already processed it,
			// or if the checkout attempt was somehow removed prematurely.
			log.Printf("CRITICAL: Checkout attempt for code %s not found in DB. Cannot process purchase.", job.Code)
			return fmt.Errorf("checkout attempt for code %s not found: %w", job.Code, err)
		}
		// Log and return other database errors.
		return fmt.Errorf("failed to get checkout attempt for code %s: %w", job.Code, err)
	}

	// Verify that the details from the job match the retrieved checkout attempt.
	// This ensures data consistency and prevents processing mismatched data.
	if job.UserID != userID || job.SaleID != saleID || job.ItemID != itemID {
		log.Printf("WARNING: Mismatch between purchase job and retrieved checkout attempt for code %s. Job: %+v, DB: UserID=%s, SaleID=%d, ItemID=%s",
			job.Code, job, userID, saleID, itemID)
		// This indicates a logical inconsistency.
		// Decide whether to return an error, attempt to correct, or log and continue.
		// Here, we return an error.
		return fmt.Errorf("purchase job data does not match checkout attempt data for code %s", job.Code)
	}

	// Begin a database transaction to ensure atomicity of the update and insert operations.
	// Both marking the checkout as used and creating the purchase record must succeed together.
	tx, err := w.PostgresManager.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for purchase code %s: %w", job.Code, err)
	}
	// Use a deferred anonymous function to handle transaction rollback in case of errors.
	// If `err` is not nil by the time this function executes, the transaction will be rolled back.
	defer func() {
		if err != nil { // This 'err' refers to the named return variable of processPurchase.
			log.Printf("INFO: Rolling back transaction for purchase code %s due to error: %v", job.Code, err)
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("ERROR: Failed to rollback transaction for code %s: %v", job.Code, rollbackErr)
			}
		}
	}()

	// Mark the retrieved checkout attempt as 'used' in the database.
	// This prevents the same checkout code from being used for multiple purchases.
	_, err = tx.Exec("UPDATE checkout_attempts SET used = TRUE WHERE id = $1", checkoutID)
	if err != nil {
		return fmt.Errorf("failed to mark checkout attempt %d as used for code %s: %w", checkoutID, job.Code, err)
	}
	log.Printf("INFO: Marked checkout attempt %d as used for code %s.", checkoutID, job.Code)

	// Create a new purchase record in the database.
	// This records the final, confirmed purchase.
	_, err = tx.Exec(
		"INSERT INTO purchases (user_id, sale_id, item_id, checkout_id, created_at) VALUES ($1, $2, $3, $4, NOW())",
		userID, saleID, itemID, checkoutID,
	)
	if err != nil {
		return fmt.Errorf("failed to create purchase record for code %s (user %s, item %s, checkout %d): %w", job.Code, userID, itemID, checkoutID, err)
	}
	log.Printf("INFO: Created purchase record for code %s.", job.Code)

	// If all operations within the transaction succeed, commit the transaction.
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction for purchase code %s: %w", job.Code, err)
	}
	log.Printf("INFO: Successfully committed purchase for code %s.", job.Code)

	return nil // Return nil if everything was successful.
}

// retryPurchase attempts to re-process a failed `PurchaseJob` after a short delay.
// This offers basic resilience against transient database or concurrency issues.
func (w *PurchaseWorker) retryPurchase(job models.PurchaseJob) {
	// Wait for a short duration before attempting the retry.
	time.Sleep(time.Second)

	log.Printf("INFO: Retrying purchase job: UserID=%s, SaleID=%d, ItemID=%s, Code=%s",
		job.UserID, job.SaleID, job.ItemID, job.Code)

	// Re-attempt to process the purchase.
	if err := w.processPurchase(job); err != nil {
		log.Printf("CRITICAL: Failed to retry purchase for job %+v after delay: %v. This purchase may require manual intervention.", job, err)
		// As with the CheckoutWorker, a more sophisticated production system
		// would implement advanced retry strategies or dead-letter queues here.
		//
	} else {
		log.Printf("INFO: Successfully retried and processed purchase for code %s.", job.Code)
	}
}
