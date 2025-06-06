package config

import (
	"context"
	"database/sql"
	"log"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

// Config holds the application configuration
type Config struct {
	RedisURL    string
	PostgresURL string
	ServerPort  string
}

// AppContext holds the application context and shared resources
type AppContext struct {
	Ctx         context.Context
	RedisClient *redis.Client
	DB          *sql.DB
	WaitGroup   *sync.WaitGroup
}

// LoadConfig loads the application configuration from environment variables
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

// InitAppContext initializes the application context and shared resources
func InitAppContext(cfg *Config) (*AppContext, error) {
	ctx := context.Background()

	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisURL,
	})

	// Test Redis connection
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("Failed to connect to Redis: %v", err)
		return nil, err
	}

	// Initialize PostgreSQL connection
	db, err := sql.Open("postgres", cfg.PostgresURL)
	if err != nil {
		log.Printf("Failed to connect to PostgreSQL: %v", err)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		log.Printf("Failed to ping PostgreSQL: %v", err)
		return nil, err
	}

	return &AppContext{
		Ctx:         ctx,
		RedisClient: redisClient,
		DB:          db,
		WaitGroup:   &sync.WaitGroup{},
	}, nil
}
