// Package config provides structures and functions for loading application
// configuration and initializing shared application resources.
package config

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// Config holds the application's configurable parameters.
// These parameters are typically loaded from environment variables.
// This allows flexible deployment across different environments.
type Config struct {
	RedisURL    string
	PostgresURL string
	ServerPort  string
}

// AppContext holds shared application-wide resources and dependencies.
// It provides a centralized point of access to components like database clients
// and a WaitGroup for managing goroutine lifecycle during graceful shutdown.
type AppContext struct {
	// Ctx is the base context for the application, typically context.Background().
	// It can be used as a parent for derived contexts with timeouts or cancellations.
	Ctx context.Context
	// RedisClient is the client for interacting with the Redis key-value store.
	RedisClient *redis.Client
	// DB is the client for interacting with the PostgreSQL relational database.
	DB *sql.DB
	// WaitGroup is used to track and synchronize the completion of background goroutines
	// during application shutdown, ensuring all tasks are finished before exiting.
	WaitGroup *sync.WaitGroup
}

// LoadConfig loads the application configuration from environment variables.
// It sets default values if environment variables are not explicitly defined,
// promoting ease of development and deployment.
//
// Environment Variables:
//   - REDIS_URL: Specifies the address for the Redis server (e.g., "localhost:6379").
//     Defaults to "localhost:6379" if not set.
//   - POSTGRES_URL: Specifies the DSN (Data Source Name) for the PostgreSQL database
//     (e.g., "postgres://user:password@host/dbname?sslmode=disable").
//     Defaults to "postgres://postgres:postgres@localhost/flashsale?sslmode=disable"
//     if not set.
//   - PORT: Specifies the HTTP server listening port (e.g., "8080").
//     Defaults to "8080" if not set.
//
// Returns:
//
//	A pointer to a Config struct containing the loaded configuration.
func LoadConfig() *Config {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		postgresURL = "postgres://postgres:postgres@localhost/flashsale?sslmode=disable"
	}

	serverPort := os.Getenv("PORT")
	if serverPort == "" {
		serverPort = "8080"
	}

	return &Config{
		RedisURL:    redisURL,
		PostgresURL: postgresURL,
		ServerPort:  serverPort,
	}
}

// InitAppContext initializes the AppContext by establishing connections to
// Redis and PostgreSQL databases based on the provided configuration.
// It performs connection checks to ensure database availability.
//
// Parameters:
//
//	cfg *Config: A pointer to the Config struct containing database connection URLs.
//
// Returns:
//
//	*AppContext: A pointer to the initialized AppContext with connected
//	             Redis and PostgreSQL clients, and a WaitGroup.
//	error: An error if any connection fails (e.g., cannot connect to Redis or PostgreSQL,
//	       or cannot ping PostgreSQL). Returns nil on successful initialization.
func InitAppContext(cfg *Config) (*AppContext, error) {
	// Use context.Background() as the root context for long-lived operations.
	ctx := context.Background()

	// Initialize Redis client with the configured URL.
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisURL,
	})

	// Ping Redis to verify the connection.
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("Failed to connect to Redis at %s: %v", cfg.RedisURL, err)
		return nil, fmt.Errorf("redis connection error: %w", err)
	}
	log.Printf("Successfully connected to Redis at %s", cfg.RedisURL)

	// Initialize PostgreSQL connection using the "postgres" driver.
	// The underscore import `_ "github.com/lib/pq"` ensures the driver
	// registers itself for sql.Open.
	db, err := sql.Open("postgres", cfg.PostgresURL)
	if err != nil {
		log.Printf("Failed to open PostgreSQL connection with DSN %s: %v", cfg.PostgresURL, err)
		return nil, fmt.Errorf("postgres connection open error: %w", err)
	}

	// Ping PostgreSQL to verify the connection.
	if err = db.Ping(); err != nil {
		log.Printf("Failed to ping PostgreSQL at %s: %v", cfg.PostgresURL, err)
		// Close the database connection if ping fails to avoid resource leaks.
		db.Close()
		return nil, fmt.Errorf("postgres ping error: %w", err)
	}
	log.Printf("Successfully connected to PostgreSQL at %s", cfg.PostgresURL)

	// Return the initialized AppContext.
	return &AppContext{
		Ctx:         ctx,
		RedisClient: redisClient,
		DB:          db,
		WaitGroup:   &sync.WaitGroup{}, // Initialize a new WaitGroup for goroutine management.
	}, nil
}
