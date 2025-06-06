package models

import (
	"time"
)

// Constants for the application
const (
	MaxItemsPerSale = 10000
	MaxItemsPerUser = 10
	CodeExpiration  = 60 * time.Minute // 1 hour TTL
)

// CheckoutResponse is the response for a checkout request
type CheckoutResponse struct {
	Code string `json:"code"`
}

// PurchaseResponse is the response for a purchase request
type PurchaseResponse struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	ItemID    string `json:"item_id,omitempty"`
	ItemName  string `json:"item_name,omitempty"`
	ItemImage string `json:"item_image,omitempty"`
}

// Item represents a product item
type Item struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Image string `json:"image"`
}

// CheckoutJob represents a job to record a checkout attempt in the database
type CheckoutJob struct {
	UserID    string
	ItemID    string
	Code      string
	SaleID    int
	CreatedAt time.Time
}

// PurchaseJob represents a job to record a purchase in the database
type PurchaseJob struct {
	UserID      string
	ItemID      string
	Code        string
	SaleID      int
	PurchasedAt time.Time
}
