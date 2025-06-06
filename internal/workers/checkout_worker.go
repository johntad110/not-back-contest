package workers

import (
	"log"
	"time"

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
)

// CheckoutWorker processes checkout jobs
type CheckoutWorker struct {
	RedisManager    *db.RedisManager
	PostgresManager *db.PostgresManager
	CheckoutChan    <-chan models.CheckoutJob
}

// NewCheckoutWorker creates a new CheckoutWorker
func NewCheckoutWorker(
	redisManager *db.RedisManager,
	postgresManager *db.PostgresManager,
	checkoutChan <-chan models.CheckoutJob,
) *CheckoutWorker {
	return &CheckoutWorker{
		RedisManager:    redisManager,
		PostgresManager: postgresManager,
		CheckoutChan:    checkoutChan,
	}
}

// Start starts the checkout worker
func (w *CheckoutWorker) Start() {
	go w.processCheckoutJobs()
}

// processCheckoutJobs processes checkout jobs from the channel
func (w *CheckoutWorker) processCheckoutJobs() {
	for job := range w.CheckoutChan {
		log.Printf("Processing checkout job: %+v", job)

		// Create a checkout attempt in the database
		_, err := w.PostgresManager.CreateCheckoutAttempt(job.UserID, job.SaleID, job.ItemID, job.Code)
		if err != nil {
			log.Printf("Failed to create checkout attempt: %v", err)
			// Retry the job
			go w.retryCheckoutJob(job)
		}
	}
}

// retryCheckoutJob retries a checkout job after a delay
func (w *CheckoutWorker) retryCheckoutJob(job models.CheckoutJob) {
	// Wait for a short time before retrying
	time.Sleep(time.Second)

	// Try to create the checkout attempt again
	_, err := w.PostgresManager.CreateCheckoutAttempt(job.UserID, job.SaleID, job.ItemID, job.Code)
	if err != nil {
		log.Printf("Failed to retry checkout attempt: %v", err)
		// Could implement more sophisticated retry logic here
	}
}
