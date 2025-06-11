package workers

import (
	"log"
	"time" // Gonna use it for implementing retry delays.

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
)

// CheckoutWorker processes asynchronous checkout jobs.
// Its primary role is to persist checkout attempt details from Redis (fast-path)
// into the PostgreSQL database (durable storage).
type CheckoutWorker struct {
	// I think `RedisManager` isleftover from a previous design (leaving it for for future expansion.)
	RedisManager *db.RedisManager
	// PostgresManager provides methods to interact with PostgreSQL,
	// where checkout attempts are recorded.
	PostgresManager *db.PostgresManager
	// CheckoutChan is a receive-only channel from which the worker
	// reads `CheckoutJob`s enqueued by the HTTP handlers.
	CheckoutChan <-chan models.CheckoutJob
}

// NewCheckoutWorker creates and returns a new instance of CheckoutWorker.
// It initializes the worker with the necessary database managers and the job channel.
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

// Start initiates the checkout worker's processing loop.
// It runs `processCheckoutJobs` in a new goroutine, allowing the worker
// to operate concurrently without blocking the main application.
func (w *CheckoutWorker) Start() {
	go w.processCheckoutJobs()
}

// processCheckoutJobs continuously reads `CheckoutJob`s from the `CheckoutChan`.
// For each job, it attempts to create a corresponding checkout attempt record
// in the PostgreSQL database. If the initial attempt fails, it schedules a retry.
func (w *CheckoutWorker) processCheckoutJobs() {
	// The `for job := range w.CheckoutChan` loop blocks until a job is available
	// on the channel, and then processes it. This loop runs indefinitely.
	for job := range w.CheckoutChan {
		// log.Printf("INFO: Processing checkout job: UserID=%s, SaleID=%d, ItemID=%s, Code=%s", job.UserID, job.SaleID, job.ItemID, job.Code)

		// Attempt to create the checkout attempt record in the PostgreSQL database.
		_, err := w.PostgresManager.CreateCheckoutAttempt(job.UserID, job.SaleID, job.ItemID, job.Code)
		if err != nil {
			log.Printf("ERROR: Failed to create checkout attempt in DB for job %+v: %v. Retrying...", job, err)
			// If persistence fails, schedule a retry of this job.
			// I used a simple `go` routine here, but a more robust production system
			// might use a separate retry queue or a backoff strategy.
			go w.retryCheckoutJob(job)
		} else {
			// log.Printf("INFO: Successfully recorded checkout attempt for code %s in DB.", job.Code)
		}
	}
}

// retryCheckoutJob attempts to re-process a failed `CheckoutJob` after a short delay.
// This provides a basic level of fault tolerance for transient database issues.
func (w *CheckoutWorker) retryCheckoutJob(job models.CheckoutJob) {
	// Introduce a small delay before retrying to avoid hammering the database
	// and potentially allow transient issues to resolve.
	time.Sleep(time.Second) // Wait for 1 second.

	log.Printf("INFO: Retrying checkout job: UserID=%s, SaleID=%d, ItemID=%s, Code=%s",
		job.UserID, job.SaleID, job.ItemID, job.Code)

	// Attempt to create the checkout attempt again.
	_, err := w.PostgresManager.CreateCheckoutAttempt(job.UserID, job.SaleID, job.ItemID, job.Code)
	if err != nil {
		log.Printf("CRITICAL: Failed to retry checkout attempt for job %+v after delay: %v. Further action may be needed (e.g., dead-letter queue).", job, err)
		// NOTE: In a real-world application, this would typically involve
		// more sophisticated retry logic (e.g., exponential backoff,
		// max retries, pushing to a dead-letter queue) rather than just a single retry.
		// (But that takes time and this is enough PoC for the contest...)
	} else {
		log.Printf("INFO: Successfully retried and recorded checkout attempt for code %s in DB.", job.Code)
	}
}
