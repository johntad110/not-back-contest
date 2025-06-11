package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/user/not-contest/internal/models" // Assumed to define constants like MaxItemsPerSale, MaxItemsPerUser, CodeExpiration
)

// CheckoutScript is a Lua script executed atomically on Redis.
// It checks if a checkout is permissible based on the overall sale limit
// and the user's purchase limit. It increments a user's checkout counter
// if the checkout is allowed, setting a 1-hour expiration.
// This script ensures that limit checks and counter increments are
// performed in a single, atomic operation to prevent race conditions.
//
// KEYS:
//
//	KEYS[1]: sale total key (e.g., "sale:<saleID>:total") - Stores the current total items sold for a sale.
//	KEYS[2]: user purchases key (e.g., "sale:<saleID>:user:<userID>:purchases") - Stores the number of items purchased by a user in a sale.
//	KEYS[3]: user checkouts key (e.g., "sale:<saleID>:user:<userID>:checkouts") - Stores the number of items a user has checked out (attempted purchase).
//
// ARGV:
//
//	ARGV[1]: MaxItemsPerSale - The maximum number of items allowed for the entire sale.
//	ARGV[2]: MaxItemsPerUser - The maximum number of items a single user can purchase in a sale.
//
// Returns:
//
//	{0, "Sale sold out"} if the sale total limit is reached.
//	{0, "User limit reached"} if the user's purchase limit is reached.
//	{1, "Success"} if the checkout is valid and the user's checkout counter is incremented.
const CheckoutScript = `
local currentSaleID = tonumber(redis.call('GET', 'current:sale:id'))
if not currentSaleID then
    return {0, "No active sale"}
end

local saleKey = "sale:"..currentSaleID..":total"
local userPurchasesKey = "sale:"..currentSaleID..":user:"..KEYS[1]..":purchases"
local userCheckoutsKey = "sale:"..currentSaleID..":user:"..KEYS[1]..":checkouts"

-- Check sale limit
local saleTotal = tonumber(redis.call('GET', saleKey) or 0)
if saleTotal >= tonumber(ARGV[1]) then
    return {0, "Sale sold out"}
end

-- Only check purchases against the limit, not checkouts, as checkouts are temporary.
local userPurchases = tonumber(redis.call('GET', userPurchasesKey) or 0)
if userPurchases >= tonumber(ARGV[2]) then
    return {0, "User limit reached"}
end

-- Still track checkouts for informational purposes
-- Increment the user's checkout counter. This tracks how many items a user has
-- attempted to checkout, which can be useful for debugging or reconciliation.
redis.call('INCR', userCheckoutsKey)
redis.call('EXPIRE', userCheckoutsKey, 3600) -- 1 hour expiration

-- Store the code if provided
if ARGV[3] ~= "" then
    local codeKey = "code:"..ARGV[3]
    local codeValue = KEYS[1]..":"..ARGV[4]..":"..currentSaleID
    redis.call('SET', codeKey, codeValue, 'EX', ARGV[5])
end

return {1, "Success", currentSaleID}
`

// PurchaseScript is a Lua script executed atomically on Redis.
// It attempts to "sell" an item by incrementing the global sale total
// and the user's purchase count, while decrementing the user's checkout
// count and deleting the used checkout code.
// This script ensures that all counter updates and code deletion are
// performed as a single atomic operation, preventing race conditions
// and ensuring accurate inventory and user limits.
//
// KEYS:
//
//	KEYS[1]: sale total key (e.g., "sale:<saleID>:total")
//	KEYS[2]: user purchases key (e.g., "sale:<saleID>:user:<userID>:purchases")
//	KEYS[3]: user checkouts key (e.g., "sale:<saleID>:user:<userID>:checkouts")
//	KEYS[4]: code key (e.g., "code:<code>") - The unique checkout code for this purchase.
//
// ARGV:
//
//	ARGV[1]: MaxItemsPerSale
//	ARGV[2]: MaxItemsPerUser
//
// Returns:
//
//	{0, "Sale sold out"} if the sale total limit is reached.
//	{0, "User purchase limit reached"} if the user's purchase limit is reached.
//	{1, "Success"} if the purchase is valid, counters are updated, and the code is deleted.
const PurchaseScript = `
local saleTotal = tonumber(redis.call('GET', KEYS[1]) or 0)
if saleTotal >= tonumber(ARGV[1]) then
  return {0, "Sale sold out"}
end

-- Check if user has reached their purchase limit before allowing the purchase.
local userPurchases = tonumber(redis.call('GET', KEYS[2]) or 0)
if userPurchases >= tonumber(ARGV[2]) then
  return {0, "User purchase limit reached"}
end

-- Increment the global sale total counter.
redis.call('INCR', KEYS[1])

-- Increment the user's purchases counter.
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], 3600) -- 1 hour expiration

-- Decrement the user's checkouts counter, as a checkout is now converting to a purchase.
local checkouts = tonumber(redis.call('GET', KEYS[3]) or 0)
if checkouts > 0 then
  redis.call('DECR', KEYS[3])
end

-- Delete the unique checkout code from Redis after it's been used.
redis.call('DEL', KEYS[4])

return {1, "Success"}
`

// RedisManager handles all interactions with the Redis key-value store.
// It provides methods for executing Lua scripts for atomic operations,
// and for managing various counters and temporary data related to sales,
// checkouts, and purchases.
type RedisManager struct {
	Client *redis.Client   // Client is the underlying go-redis client.
	Ctx    context.Context // Ctx is the base context for Redis operations.
}

// NewRedisManager creates and returns a new instance of RedisManager.
// It takes an initialized go-redis client and a context as arguments.
func NewRedisManager(client *redis.Client, ctx context.Context) *RedisManager {
	return &RedisManager{
		Client: client,
		Ctx:    ctx,
	}
}

// ExecuteCheckoutScript executes the `CheckoutScript` atomically on Redis.
// This script is responsible for validating a checkout attempt against
// global sale limits and user-specific purchase limits, then incrementing
// the user's checkout counter.
//
// Parameters:
//
//	saleID int: The ID of the current flash sale.
//	userID string: The ID of the user attempting to checkout.
//
// Returns:
//
//	bool: True if the checkout is successful and allowed, false otherwise.
//	string: A message indicating the reason for success or failure (e.g., "Success", "Sale sold out").
//	error: An error if the Redis script execution fails, otherwise nil.
func (rm *RedisManager) ExecuteCheckoutScript(userID, itemID, generatedCode string) (bool, string, int, error) {
	// Execute the pre-defined Lua script with the necessary keys and arguments.
	result, err := rm.Client.Eval(rm.Ctx, CheckoutScript, []string{
		userID, // KEYS[1]
	}, models.MaxItemsPerSale, models.MaxItemsPerUser, generatedCode, itemID, int(models.CodeExpiration.Seconds())).Result()

	if err != nil {
		log.Printf("Error executing checkout script for user %s: %v", userID, err)
		return false, "", 0, fmt.Errorf("error executing checkout script: %w", err)
	}

	// Parse the script's return value, which is expected to be a slice of interfaces.
	resultArray, ok := result.([]interface{})
	if !ok || len(resultArray) < 3 {
		log.Printf("Invalid result format from checkout script: %v", result)
		return false, "", 0, fmt.Errorf("invalid result format from checkout script")
	}

	success, _ := resultArray[0].(int64)
	message, _ := resultArray[1].(string)
	saleID, _ := resultArray[2].(int64)

	return success == 1, message, int(saleID), nil
}

// ExecutePurchaseScript executes the `PurchaseScript` atomically on Redis.
// This script is crucial for completing a purchase: it increments sale and user
// purchase counters, decrements user checkout counters, and removes the used code.
//
// Parameters:
//
//	saleID int: The ID of the current flash sale.
//	userID string: The ID of the user making the purchase.
//	code string: The unique checkout code being used for this purchase.
//
// Returns:
//
//	bool: True if the purchase is successful and allowed, false otherwise.
//	string: A message indicating the reason for success or failure (e.g., "Success", "Sale sold out").
//	error: An error if the Redis script execution fails, otherwise nil.
func (rm *RedisManager) ExecutePurchaseScript(saleID int, userID, code string) (bool, string, error) {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	codeKey := fmt.Sprintf("code:%s", code) // Key for the checkout code.

	// Execute the pre-defined Lua script for purchase.
	result, err := rm.Client.Eval(rm.Ctx, PurchaseScript, []string{
		saleKey,
		userPurchasesKey,
		userCheckoutsKey,
		codeKey,
	}, models.MaxItemsPerSale, models.MaxItemsPerUser).Result()

	if err != nil {
		log.Printf("Error executing purchase script for user %s, code %s: %v", userID, code, err)
		return false, "", fmt.Errorf("error executing purchase script: %w", err)
	}

	// Parse the script's return value.
	resultArray, ok := result.([]interface{})
	if !ok || len(resultArray) < 2 {
		log.Printf("Invalid result format from purchase script: %v", result)
		return false, "", fmt.Errorf("invalid result format from purchase script")
	}

	success, _ := resultArray[0].(int64)
	message, _ := resultArray[1].(string)

	return success == 1, message, nil
}

// StoreCode stores a unique checkout code in Redis, associated with the user, item,
// and sale ID. It sets an expiration time defined by `models.CodeExpiration`.
// This acts as a temporary token for a pending purchase.
//
// Parameters:
//
//	code string: The unique checkout code.
//	userID string: The ID of the user who received the code.
//	itemID string: The ID of the item associated with the code.
//	saleID int: The ID of the sale the code belongs to.
//
// Returns:
//
//	error: An error if the Redis SET operation fails, otherwise nil.
func (rm *RedisManager) StoreCode(code, userID, itemID string, saleID int) error {
	codeKey := fmt.Sprintf("code:%s", code)
	codeValue := fmt.Sprintf("%s:%s:%d", userID, itemID, saleID) // Combine info into a single string.
	err := rm.Client.Set(rm.Ctx, codeKey, codeValue, models.CodeExpiration).Err()
	if err != nil {
		log.Printf("Failed to store code %s in Redis: %v", code, err)
		return fmt.Errorf("failed to store code: %w", err)
	}
	return nil
}

// GetCodeValue retrieves the associated value for a given checkout code from Redis.
// The value contains combined user ID, item ID, and sale ID.
//
// Parameters:
//
//	code string: The unique checkout code.
//
// Returns:
//
//	string: The combined value (e.g., "userID:itemID:saleID") if the code exists.
//	error: An error if the Redis GET operation fails or the key does not exist.
func (rm *RedisManager) GetCodeValue(code string) (string, error) {
	codeKey := fmt.Sprintf("code:%s", code)
	val, err := rm.Client.Get(rm.Ctx, codeKey).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("code %s not found in Redis", code)
		}
		log.Printf("Failed to get code value for %s from Redis: %v", code, err)
		return "", fmt.Errorf("failed to get code value: %w", err)
	}
	return val, nil
}

// SetSaleTotal sets the total count of items sold for a specific sale in Redis.
// This is used for initialization and reconciliation.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//	total int: The total number of items to set.
//
// Returns:
//
//	error: An error if the Redis SET operation fails, otherwise nil.
func (rm *RedisManager) SetSaleTotal(saleID, total int) error {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	err := rm.Client.Set(rm.Ctx, saleKey, total, 0).Err() // 0 means no expiration.
	if err != nil {
		log.Printf("Failed to set sale total for sale %d to %d in Redis: %v", saleID, total, err)
		return fmt.Errorf("failed to set sale total: %w", err)
	}
	return nil
}

// GetSaleTotal retrieves the total count of items sold for a specific sale from Redis.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//
// Returns:
//
//	int: The total count of items sold.
//	error: An error if the Redis GET operation fails or the key does not exist.
func (rm *RedisManager) GetSaleTotal(saleID int) (int, error) {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	val, err := rm.Client.Get(rm.Ctx, saleKey).Int()
	if err != nil {
		if err == redis.Nil {
			return 0, nil // Key not found, assume 0 total.
		}
		log.Printf("Failed to get sale total for sale %d from Redis: %v", saleID, err)
		return 0, fmt.Errorf("failed to get sale total: %w", err)
	}
	return val, nil
}

// SetUserCheckouts sets the number of checkout attempts for a specific user in a sale in Redis.
// This is used for initialization, reconciliation,
// The key has a 1-hour expiration.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//	userID string: The ID of the user.
//	count int: The number of checkouts to set.
//
// Returns:
//
//	error: An error if the Redis SET operation fails, otherwise nil.
func (rm *RedisManager) SetUserCheckouts(saleID int, userID string, count int) error {
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	err := rm.Client.Set(rm.Ctx, userCheckoutsKey, count, time.Hour).Err()
	if err != nil {
		log.Printf("Failed to set user checkouts for user %s in sale %d to %d in Redis: %v", userID, saleID, count, err)
		return fmt.Errorf("failed to set user checkouts: %w", err)
	}
	return nil
}

// GetUserCheckouts retrieves the number of checkout attempts for a specific user in a sale from Redis.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//	userID string: The ID of the user.
//
// Returns:
//
//	int: The number of checkout attempts.
//	error: An error if the Redis GET operation fails or the key does not exist.
func (rm *RedisManager) GetUserCheckouts(saleID int, userID string) (int, error) {
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	val, err := rm.Client.Get(rm.Ctx, userCheckoutsKey).Int()
	if err != nil {
		if err == redis.Nil {
			return 0, nil // Key not found, assume 0 checkouts.
		}
		log.Printf("Failed to get user checkouts for user %s in sale %d from Redis: %v", userID, saleID, err)
		return 0, fmt.Errorf("failed to get user checkouts: %w", err)
	}
	return val, nil
}

// SetUserPurchases sets the number of purchases for a specific user in a sale in Redis.
// This is used for initialization, reconciliation,
// The key has a 1-hour expiration.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//	userID string: The ID of the user.
//	count int: The number of purchases to set.
//
// Returns:
//
//	error: An error if the Redis SET operation fails, otherwise nil.
func (rm *RedisManager) SetUserPurchases(saleID int, userID string, count int) error {
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	err := rm.Client.Set(rm.Ctx, userPurchasesKey, count, time.Hour).Err()
	if err != nil {
		log.Printf("Failed to set user purchases for user %s in sale %d to %d in Redis: %v", userID, saleID, count, err)
		return fmt.Errorf("failed to set user purchases: %w", err)
	}
	return nil
}

// GetUserPurchases retrieves the number of purchases for a specific user in a sale from Redis.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//	userID string: The ID of the user.
//
// Returns:
//
//	int: The number of purchases.
//	error: An error if the Redis GET operation fails or the key does not exist.
func (rm *RedisManager) GetUserPurchases(saleID int, userID string) (int, error) {
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	val, err := rm.Client.Get(rm.Ctx, userPurchasesKey).Int()
	if err != nil {
		if err == redis.Nil {
			return 0, nil // Key not found, assume 0 purchases.
		}
		log.Printf("Failed to get user purchases for user %s in sale %d from Redis: %v", userID, saleID, err)
		return 0, fmt.Errorf("failed to get user purchases: %w", err)
	}
	return val, nil
}

// IncrementUserCheckouts atomically increments the number of checkout attempts for a user in a sale.
// The key is set with a 1-hour expiration.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//	userID string: The ID of the user.
//
// Returns:
//
//	error: An error if the Redis INCR or EXPIRE operation fails, otherwise nil.
func (rm *RedisManager) IncrementUserCheckouts(saleID int, userID string) error {
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	_, err := rm.Client.Incr(rm.Ctx, userCheckoutsKey).Result()
	if err != nil {
		log.Printf("Failed to increment user checkouts for user %s in sale %d: %v", userID, saleID, err)
		return fmt.Errorf("failed to increment user checkouts: %w", err)
	}
	// Ensure the key has an expiration, even if it already existed.
	return rm.Client.Expire(rm.Ctx, userCheckoutsKey, time.Hour).Err()
}

// DecrementUserCheckouts atomically decrements the number of checkout attempts for a user in a sale.
// This is typically called after a checkout attempt converts to a purchase.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//	userID string: The ID of the user.
//
// Returns:
//
//	error: An error if the Redis DECR operation fails, otherwise nil.
func (rm *RedisManager) DecrementUserCheckouts(saleID int, userID string) error {
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	err := rm.Client.Decr(rm.Ctx, userCheckoutsKey).Err()
	if err != nil {
		log.Printf("Failed to decrement user checkouts for user %s in sale %d: %v", userID, saleID, err)
		return fmt.Errorf("failed to decrement user checkouts: %w", err)
	}
	return nil
}

// IncrementUserPurchases atomically increments the number of successful purchases for a user in a sale.
// The key is set with a 1-hour expiration.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//	userID string: The ID of the user.
//
// Returns:
//
//	error: An error if the Redis INCR or EXPIRE operation fails, otherwise nil.
func (rm *RedisManager) IncrementUserPurchases(saleID int, userID string) error {
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	_, err := rm.Client.Incr(rm.Ctx, userPurchasesKey).Result()
	if err != nil {
		log.Printf("Failed to increment user purchases for user %s in sale %d: %v", userID, saleID, err)
		return fmt.Errorf("failed to increment user purchases: %w", err)
	}
	// Ensure the key has an expiration, even if it already existed.
	return rm.Client.Expire(rm.Ctx, userPurchasesKey, time.Hour).Err()
}

// IncrementSaleTotal atomically increments the total number of items sold for a given sale.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//
// Returns:
//
//	error: An error if the Redis INCR operation fails, otherwise nil.
func (rm *RedisManager) IncrementSaleTotal(saleID int) error {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	err := rm.Client.Incr(rm.Ctx, saleKey).Err()
	if err != nil {
		log.Printf("Failed to increment sale total for sale %d: %v", saleID, err)
		return fmt.Errorf("failed to increment sale total: %w", err)
	}
	return nil
}

// DeleteCode removes a specific checkout code from Redis.
// This is typically called after a code has been successfully used for a purchase.
//
// Parameters:
//
//	code string: The unique checkout code to delete.
//
// Returns:
//
//	error: An error if the Redis DEL operation fails, otherwise nil.
func (rm *RedisManager) DeleteCode(code string) error {
	codeKey := fmt.Sprintf("code:%s", code)
	err := rm.Client.Del(rm.Ctx, codeKey).Err()
	if err != nil {
		log.Printf("Failed to delete code %s from Redis: %v", code, err)
		return fmt.Errorf("failed to delete code: %w", err)
	}
	return nil
}

// GetAllCodes retrieves all keys in Redis that match the "code:*" pattern.
// This is primarily for reconciliation purposes to identify lingering or unused codes.
//
// Returns:
//
//	[]string: A slice of all existing checkout code keys.
//	error: An error if the Redis KEYS command fails, otherwise nil.
func (rm *RedisManager) GetAllCodes() ([]string, error) {
	codes, err := rm.Client.Keys(rm.Ctx, "code:*").Result()
	if err != nil {
		log.Printf("Failed to get all code keys from Redis: %v", err)
		return nil, fmt.Errorf("failed to get all codes: %w", err)
	}
	return codes, nil
}

// GetAllUserPurchaseKeys retrieves all user purchase keys for a specific sale ID from Redis.
// This is used for reconciliation or auditing to get a comprehensive view of user purchases.
//
// Parameters:
//
//	saleID int: The ID of the sale.
//
// Returns:
//
//	[]string: A slice of all user purchase keys for the specified sale.
//	error: An error if the Redis KEYS command fails, otherwise nil.
func (rm *RedisManager) GetAllUserPurchaseKeys(saleID int) ([]string, error) {
	// Note: Using KEYS in production is generally discouraged for large datasets as it can block the server.
	// For reconciliation or smaller datasets, its usage might be acceptable. (We used it here for reconcilation.)
	keys, err := rm.Client.Keys(rm.Ctx, fmt.Sprintf("sale:%d:user:*:purchases", saleID)).Result()
	if err != nil {
		log.Printf("Failed to get all user purchase keys for sale %d from Redis: %v", saleID, err)
		return nil, fmt.Errorf("failed to get all user purchase keys: %w", err)
	}
	return keys, nil
}

func (rm *RedisManager) SetCurrentSaleID(saleID int) error {
	return rm.Client.Set(rm.Ctx, "current:sale:id", saleID, 0).Err()
}

func (rm *RedisManager) InitializeSaleCounters(saleID int) error {
	totalKey := fmt.Sprintf("sale:%d:total", saleID)
	return rm.Client.Set(rm.Ctx, totalKey, 0, time.Hour).Err()
}
