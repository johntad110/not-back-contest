package models

import (
	"time"
)

// Constants define key configuration values and limits for the flash-sale application.
const (
	// MaxItemsPerSale specifies the maximum total number of items that can be sold in a single flash sale.
	// This acts as a global inventory limit for the sale.
	MaxItemsPerSale = 10000
	// MaxItemsPerUser specifies the maximum number of items a single user can purchase in a given sale.
	// This helps prevent a few users from monopolizing the sale.
	MaxItemsPerUser = 10
	// CodeExpiration defines the time-to-live (TTL) for a checkout code stored in Redis.
	// A user must complete their purchase within this duration after receiving a checkout code.
	CodeExpiration = 60 * time.Minute // 1 hour TTL
)

// CheckoutResponse represents the structure of the JSON response sent to a client
// after a successful checkout attempt. It provides the unique code required
// for the subsequent purchase step.
type CheckoutResponse struct {
	Code string `json:"code"` // The unique code generated for the checkout attempt.
}

// PurchaseResponse represents the structure of the JSON response sent to a client
// after a successful purchase. It confirms the purchase and provides details
// about the purchased item.
type PurchaseResponse struct {
	Success   bool   `json:"success"`              // Indicates if the purchase was successful.
	Message   string `json:"message"`              // A descriptive message about the purchase outcome. Blah blah
	ItemID    string `json:"item_id,omitempty"`    // The unique identifier of the purchased item.
	ItemName  string `json:"item_name,omitempty"`  // The name of the purchased item.
	ItemImage string `json:"item_image,omitempty"` // The URL or path to the image of the purchased item.
}

// Item represents the basic structure of a product item available in a sale.
type Item struct {
	ID    string `json:"id"`    // The unique identifier for the item.
	Name  string `json:"name"`  // The human-readable name of the item.
	Image string `json:"image"` // The URL or path to the item's image.
}

// CheckoutJob represents a unit of work to be processed by a background worker
// for persisting a checkout attempt into the PostgreSQL database.
// It carries all necessary information from the initial checkout request.
type CheckoutJob struct {
	UserID    string    // The ID of the user who initiated the checkout.
	ItemID    string    // The ID of the item being checked out.
	Code      string    // The unique checkout code generated for this attempt.
	SaleID    int       // The ID of the sale this checkout belongs to.
	CreatedAt time.Time // The timestamp when the checkout job was created (usually reflects request time).
}

// PurchaseJob represents a unit of work to be processed by a background worker
// for persisting a successful purchase into the PostgreSQL database.
// It contains details of the confirmed transaction.
type PurchaseJob struct {
	UserID      string    // The ID of the user who completed the purchase.
	ItemID      string    // The ID of the item purchased.
	Code        string    // The unique checkout code used for this purchase.
	SaleID      int       // The ID of the sale this purchase belongs to.
	PurchasedAt time.Time // The timestamp when the purchase was confirmed.
}
