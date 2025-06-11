// Package main is the entry point for the flash-sale service.
// It initializes and orchestrates various components of the application,
// including configuration, database connections, background workers,
// and HTTP handlers, to provide a robust and high-throughput flash-sale system.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/user/not-contest/internal/config"
	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/handlers"
	"github.com/user/not-contest/internal/models"
	"github.com/user/not-contest/internal/service"
	"github.com/user/not-contest/internal/workers"
)

// main function serves as the application's entry point.
// It performs the following key steps:
//  1. Loads application configuration.
//  2. Initializes the application context, including Redis and PostgreSQL clients.
//  3. Initializes database managers for Redis and PostgreSQL.
//  4. Ensures necessary database tables are created.
//  5. Sets up channels for inter-goroutine communication for checkout and purchase jobs.
//  6. Initializes core services: SaleService for managing sale state and
//     ReconciliationService for ensuring data consistency.
//  7. Initializes background workers: CheckoutWorker and PurchaseWorker,
//     which process asynchronous checkout and purchase requests.
//  8. Initializes HTTP handlers for `/checkout` and `/purchase` endpoints.
//  9. Starts all background workers and services.
//  10. Verifies and ensures the existence of the current flash sale in the database.
//  11. Configures and starts the HTTP server to handle incoming requests.
//  12. Implements graceful shutdown procedures, ensuring all resources are
//     released and pending operations are completed before exiting.
func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Initialize the application context, which holds shared resources like
	// database connections and a WaitGroup for graceful shutdown.
	appCtx, err := config.InitAppContext(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize application context: %v", err)
	}

	// Initialize database managers
	redisManager := db.NewRedisManager(appCtx.RedisClient, appCtx.Ctx)
	postgresManager := db.NewPostgresManager(appCtx.DB)

	// Initialize database tables. This idempotent operation ensures the schema
	// is present for the application to function correctly.
	if initErr := postgresManager.InitializeTables(); initErr != nil {
		log.Fatalf("Failed to initialize database tables: %v", err)
	}

	// Create buffered channels for dispatching checkout and purchase jobs
	// to their respective background workers. The buffer size helps in
	// absorbing bursts of requests.
	checkoutJobChan := make(chan models.CheckoutJob, 100)
	purchaseJobChan := make(chan models.PurchaseJob, 100)

	// Initialize core business logic services.
	saleService := service.NewSaleService(redisManager, postgresManager)
	reconciliationService := service.NewReconciliationService(redisManager, postgresManager)

	// Initialize background workers. These workers process jobs from channels
	// asynchronously, offloading heavy lifting from HTTP request handlers.
	checkoutWorker := workers.NewCheckoutWorker(redisManager, postgresManager, checkoutJobChan)
	purchaseWorker := workers.NewPurchaseWorker(redisManager, postgresManager, purchaseJobChan)

	// Initialize HTTP handlers. These handlers receive incoming HTTP requests
	// and delegate processing to services and workers.
	checkoutHandler := handlers.NewCheckoutHandler(redisManager, postgresManager, saleService, checkoutJobChan)
	purchaseHandler := handlers.NewPurchaseHandler(redisManager, postgresManager, saleService, purchaseJobChan)

	// Start all background workers as goroutines. These will run concurrently
	// with the HTTP server, processing jobs as they arrive.
	checkoutWorker.Start()
	purchaseWorker.Start()

	// Start services
	saleService.StartSaleManagement()
	reconciliationService.StartReconciliation()

	// Ensure the current sale exists in the database. This is critical for
	// maintaining sale consistency across restarts and ensuring items are
	// correctly associated with an active sale.
	currentSaleID, err := saleService.GetCurrentSaleID()
	if err != nil {
		log.Fatalf("Failed to get current sale ID: %v", err)
	}
	if err := postgresManager.EnsureSaleExists(currentSaleID); err != nil {
		log.Fatalf("Failed to ensure sale exists: %v", err)
	}
	// The above ensurer created the sale if it doesn't exist
	// And if we create it we need to update redis
	if err := redisManager.SetCurrentSaleID(currentSaleID); err != nil {
		log.Printf("ERROR: Failed to update current sale ID in Redis: %v", err)
	}

	// Set up the HTTP server multiplexer and register the handlers
	// for the defined API endpoints.
	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", checkoutHandler.Handle)
	mux.HandleFunc("/purchase", purchaseHandler.Handle)

	// Configure the HTTP server with the listening address and handlers.
	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.ServerPort),
		Handler: mux,
	}

	// Start the HTTP server in a non-blocking goroutine.
	go func() {
		log.Printf("Server listening on port %s", cfg.ServerPort)
		// ListenAndServe returns http.ErrServerClosed when Shutdown is called.
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Set up a channel to listen for OS interrupt signals (SIGINT, SIGTERM)
	// for graceful shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Block until an interrupt signal is received.
	<-sigChan
	log.Println("Shutting down server...")

	// Create a deadline (context with a timeout) for graceful server shutdown.
	// This gives active requests a chance to complete.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() // Release resources associated with this context.

	// Attempt to gracefully shut down the HTTP server.
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}

	// Close job channels. This signals to workers that no more jobs will be sent.
	// Workers handle this by finishing any pending jobs and then they exit.
	close(checkoutJobChan)
	close(purchaseJobChan)

	// Wait for all goroutines (started by the application context's WaitGroup)
	// to finish their execution. This ensures all background tasks complete
	// before the application exits.
	appCtx.WaitGroup.Wait()

	// Close database connections to release resources held by Redis and PostgreSQL clients.
	appCtx.RedisClient.Close()
	appCtx.DB.Close()

	log.Println("Server stopped gracefully")
}
