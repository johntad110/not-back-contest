package service

import (
	"log"
	"math/rand"
	"time"

	"github.com/user/not-contest/internal/db"
	"github.com/user/not-contest/internal/models"
)

// SaleService handles sale-related operations
type SaleService struct {
	RedisManager    *db.RedisManager
	PostgresManager *db.PostgresManager
}

// NewSaleService creates a new SaleService
func NewSaleService(
	redisManager *db.RedisManager,
	postgresManager *db.PostgresManager,
) *SaleService {
	return &SaleService{
		RedisManager:    redisManager,
		PostgresManager: postgresManager,
	}
}

// StartSaleManagement starts the sale management process
func (s *SaleService) StartSaleManagement() {
	go s.manageSales()
}

// GetCurrentSaleID gets the ID of the current sale
func (s *SaleService) GetCurrentSaleID() (int, error) {
	return s.PostgresManager.GetCurrentSaleID()
}

// manageSales manages the creation of new sales
func (s *SaleService) manageSales() {
	// Create a new sale every hour
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		// End the current sale
		currentSaleID, err := s.PostgresManager.GetCurrentSaleID()
		if err != nil {
			log.Printf("Failed to get current sale ID: %v", err)
			continue
		}

		if endErr := s.PostgresManager.EndSale(currentSaleID); endErr != nil {
			log.Printf("Failed to end sale: %v", err)
			continue
		}

		// Create a new sale
		newSaleID, err := s.PostgresManager.CreateSale()
		if err != nil {
			log.Printf("Failed to create new sale: %v", err)
			continue
		}

		log.Printf("Created new sale with ID %d", newSaleID)
	}
}

// GenerateItem generates a random item
func (s *SaleService) GenerateItem(itemID string) models.Item {
	// List of possible item names
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

	// List of possible image URLs
	imageURLs := []string{
		"https://not-contest-cdn.openbuilders.xyz/items/1.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/2.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/3.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/5.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/6.png",
		"https://not-contest-cdn.openbuilders.xyz/items/7.jpg",
		"https://not-contest-cdn.openbuilders.xyz/items/8.jpg",
	}

	// Generate a random item
	name := itemNames[rand.Intn(len(itemNames))]
	imageURL := imageURLs[rand.Intn(len(imageURLs))]
	// price := fmt.Sprintf("$%.2f", 10.0+rand.Float64()*90.0)

	return models.Item{
		ID:    itemID,
		Name:  name,
		Image: imageURL,
	}
}
