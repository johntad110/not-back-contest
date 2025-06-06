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

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Initialize application context
	appCtx, err := config.InitAppContext(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize application context: %v", err)
	}

	// Initialize database managers
	redisManager := db.NewRedisManager(appCtx.RedisClient, appCtx.Ctx)
	postgresManager := db.NewPostgresManager(appCtx.DB)

	// Initialize database tables
	if initErr := postgresManager.InitializeTables(); initErr != nil {
		log.Fatalf("Failed to initialize database tables: %v", err)
	}

	// Create channels for background workers
	checkoutJobChan := make(chan models.CheckoutJob, 100)
	purchaseJobChan := make(chan models.PurchaseJob, 100)

	// Initialize services
	saleService := service.NewSaleService(redisManager, postgresManager)
	reconciliationService := service.NewReconciliationService(redisManager, postgresManager)

	// Initialize workers
	checkoutWorker := workers.NewCheckoutWorker(redisManager, postgresManager, checkoutJobChan)
	purchaseWorker := workers.NewPurchaseWorker(redisManager, postgresManager, purchaseJobChan)

	// Initialize handlers
	checkoutHandler := handlers.NewCheckoutHandler(redisManager, postgresManager, saleService, checkoutJobChan)
	purchaseHandler := handlers.NewPurchaseHandler(redisManager, postgresManager, saleService, purchaseJobChan)

	// Start background workers
	checkoutWorker.Start()
	purchaseWorker.Start()

	// Start services
	saleService.StartSaleManagement()
	reconciliationService.StartReconciliation()

	// Ensure the current sale exists
	currentSaleID, err := saleService.GetCurrentSaleID()
	if err != nil {
		log.Fatalf("Failed to get current sale ID: %v", err)
	}
	if err := postgresManager.EnsureSaleExists(currentSaleID); err != nil {
		log.Fatalf("Failed to ensure sale exists: %v", err)
	}

	// Set up HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", checkoutHandler.Handle)
	mux.HandleFunc("/purchase", purchaseHandler.Handle)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.ServerPort),
		Handler: mux,
	}

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("Server listening on port %s", cfg.ServerPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for interrupt signal
	<-sigChan
	log.Println("Shutting down server...")

	// Create a deadline for the shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shutdown the server
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}

	// Close channels
	close(checkoutJobChan)
	close(purchaseJobChan)

	// Wait for all goroutines to finish
	appCtx.WaitGroup.Wait()

	// Close database connections
	appCtx.RedisClient.Close()
	appCtx.DB.Close()

	log.Println("Server stopped gracefully")
}
