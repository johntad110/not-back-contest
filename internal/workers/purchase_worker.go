package workers

import (
	"log"
	"time"

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
)

// PurchaseWorker processes purchase jobs
type PurchaseWorker struct {
	RedisManager    *db.RedisManager
	PostgresManager *db.PostgresManager
	PurchaseChan    <-chan models.PurchaseJob
}

// NewPurchaseWorker creates a new PurchaseWorker
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

// Start starts the purchase worker
func (w *PurchaseWorker) Start() {
	go w.processPurchaseJobs()
}

// processPurchaseJobs processes purchase jobs from the channel
func (w *PurchaseWorker) processPurchaseJobs() {
	for job := range w.PurchaseChan {
		log.Printf("Processing purchase job: %+v", job)

		// Process the purchase
		if err := w.processPurchase(job); err != nil {
			log.Printf("Failed to process purchase: %v", err)
		}
	}
}

// processPurchase processes a purchase job
func (w *PurchaseWorker) processPurchase(job models.PurchaseJob) error {
	// Get the checkout attempt
	checkoutID, userID, saleID, itemID, err := w.PostgresManager.GetCheckoutAttempt(job.Code)
	if err != nil {
		log.Printf("Failed to get checkout attempt: %v", err)
		return err
	}

	// Verify that the job matches the checkout attempt
	if job.UserID != userID || job.SaleID != saleID || job.ItemID != itemID {
		log.Printf("Job does not match checkout attempt: %+v vs %s, %d, %s", job, userID, saleID, itemID)
		return err
	}

	// Begin a transaction
	tx, err := w.PostgresManager.BeginTransaction()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Mark the checkout attempt as used
	_, err = tx.Exec("UPDATE checkout_attempts SET used = TRUE WHERE id = $1", checkoutID)
	if err != nil {
		log.Printf("Failed to mark checkout attempt as used: %v", err)
		return err
	}

	// Create a purchase record
	_, err = tx.Exec(
		"INSERT INTO purchases (user_id, sale_id, item_id, checkout_id, created_at) VALUES ($1, $2, $3, $4, NOW())",
		userID, saleID, itemID, checkoutID,
	)
	if err != nil {
		log.Printf("Failed to create purchase record: %v", err)
		return err
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return err
	}

	return nil
}

// retryPurchase retries a purchase job after a delay
func (w *PurchaseWorker) retryPurchase(job models.PurchaseJob) {
	// Wait for a short time before retrying
	time.Sleep(time.Second)

	// Try to process the purchase again
	if err := w.processPurchase(job); err != nil {
		log.Printf("Failed to retry purchase: %v", err)
		// Could implement more sophisticated retry logic here
	}
}
