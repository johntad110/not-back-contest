package service

import (
	"log"
	"math/rand"
	"time"

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
)

// SaleService handles all business logic related to managing flash sales,
// including their creation, termination, and providing current sale information.
type SaleService struct {
	// RedisManager is used here primarily for managing any Redis-side sale data,
	// though its primary role in `manageSales` is to coordinate with PostgresManager.
	RedisManager *db.RedisManager
	// PostgresManager provides methods for interacting with PostgreSQL,
	// which is the authoritative source for sale lifecycle and current sale ID.
	PostgresManager *db.PostgresManager
}

// NewSaleService creates and returns a new instance of SaleService.
// It takes RedisManager and PostgresManager as dependencies.
func NewSaleService(
	redisManager *db.RedisManager,
	postgresManager *db.PostgresManager,
) *SaleService {
	return &SaleService{
		RedisManager:    redisManager,
		PostgresManager: postgresManager,
	}
}

// StartSaleManagement initiates the background process for managing sales.
// It runs the `manageSales` method in a new goroutine to operate asynchronously.
func (s *SaleService) StartSaleManagement() {
	go s.manageSales()
}

// GetCurrentSaleID retrieves the ID of the currently active flash sale
// from the PostgreSQL database.
func (s *SaleService) GetCurrentSaleID() (int, error) {
	return s.PostgresManager.GetCurrentSaleID()
}

// manageSales is a long-running goroutine that periodically manages the
// lifecycle of flash sales. It's configured to end the current sale and
// create a new one at a fixed interval (e.g., every hour).
func (s *SaleService) manageSales() {
	// Create a ticker that fires once every hour.
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop() // Ensure the ticker is stopped when the function exits.

	// The loop will execute every time the ticker sends a signal.
	for range ticker.C {
		log.Println("Initiating sale management cycle.")

		// Get the ID of the current active sale to end it.
		currentSaleID, err := s.PostgresManager.GetCurrentSaleID()
		if err != nil {
			log.Printf("ERROR: Failed to get current sale ID during management: %v. Skipping sale ending.", err)
			continue // Continue to the next tick if current sale ID cannot be retrieved.
		}

		if endErr := s.PostgresManager.EndSale(currentSaleID); endErr != nil {
			log.Printf("ERROR: Failed to end sale %d: %v. Continuing to next step.", currentSaleID, endErr)
			continue
		} else {
			log.Printf("INFO: Successfully ended sale with ID %d.", currentSaleID)
		}

		// Create a new flash sale record in the PostgreSQL database.
		newSaleID, err := s.PostgresManager.CreateSale()
		if err != nil {
			log.Printf("ERROR: Failed to create new sale: %v. Skipping this cycle.", err)
			continue
		}

		// IMPORTANT: After creating a new sale in Postgres, it's critical to
		// also reset Redis state for the new sale. This typically involves:
		// 1. Initializing the `sale:<newSaleID>:items_available` counter.
		// 2. Clearing any user-specific counters for the previous sale if they are not expired by TTL.
		// 3. Potentially clearing all checkout codes related to the previous sale if they weren't used.
		// My current code *does not* explicitly perform these Redis resets here,
		// which could lead to stale data. But Redis automatically handle
		// expirations plus reconciliation happens immediately (in about 10 sec).
		// A potential addition here would be (will consider it in the future):
		// if err := s.RedisManager.InitializeSale(newSaleID, models.MaxItemsPerSale); err != nil {
		//     log.Printf("WARNING: Failed to initialize new sale %d in Redis: %v", newSaleID, err)
		// }
		// And similarly for clearing old user checkouts/purchases from Redis.

		// Update Redis with new sale ID
		if err := s.RedisManager.SetCurrentSaleID(newSaleID); err != nil {
			log.Printf("ERROR: Failed to update current sale ID in Redis: %v", err)
			continue
		}

		// Initialize Redis counters for new sale
		if err := s.RedisManager.InitializeSaleCounters(newSaleID); err != nil {
			log.Printf("ERROR: Failed to initialize Redis counters for sale %d: %v", newSaleID, err)
		}

		log.Printf("INFO: Successfully created new sale with ID %d and updated Redis.", newSaleID)
	}
}

// GenerateItem creates a synthetic `models.Item` based on a given item ID.
// This function is used to provide item details for responses.
// It randomly selects a name and image from predefined lists.
func (s *SaleService) GenerateItem(itemID string) models.Item {
	// Predefined (😉) list of possible item names for generating synthetic item data.
	itemNames := []string{
		"Vintage Vinyl Record",
		"Handcrafted Wooden Bowl",
		"Artisanal Chocolate Bar",
		"Limited Edition Print",
		"Handmade Ceramic Mug",
		"Organic Cotton T-Shirt",
		"Leather-Bound Journal",
		"Scented Soy Candle",
		"Craft Beer Sampler",
		"Gourmet Coffee Beans",
	}

	// Predefined list of possible image URLs for generating synthetic item data.
	imageURLs := []string{
		"https://not-contest-cdn.openbuilders.xyz/items/1.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/2.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/3.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/5.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/6.png",
		"https://not-contest-cdn.openbuilders.xyz/items/7.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/8.jpg",
	}

	// Use a random index to pick a name and an image URL.
	// `rand.Intn` generates a random integer between 0 (inclusive) and the argument (exclusive).
	name := itemNames[rand.Intn(len(itemNames))]
	imageURL := imageURLs[rand.Intn(len(imageURLs))]
	// price := fmt.Sprintf("$%.2f", 10.0+rand.Float64()*90.0)

	// Construct and return the `models.Item` with the provided `itemID` and generated details.
	return models.Item{
		ID:    itemID,
		Name:  name,
		Image: imageURL,
	}
}
