package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/user/not-contest/internal/models"
)

// Redis Lua script for atomic checkout validation and counter increment
// KEYS[1] = sale total key (sale:<saleID>:total)
// KEYS[2] = user purchases key (sale:<saleID>:user:<userID>:purchases)
// KEYS[3] = user checkouts key (sale:<saleID>:user:<userID>:checkouts)
// ARGV[1] = MaxItemsPerSale
// ARGV[2] = MaxItemsPerUser
const CheckoutScript = `
local saleTotal = tonumber(redis.call('GET', KEYS[1]) or 0)
if saleTotal >= tonumber(ARGV[1]) then
  return {0, "Sale sold out"}
end

-- Only check purchases against the limit, not checkouts
local userPurchases = tonumber(redis.call('GET', KEYS[2]) or 0)
if userPurchases >= tonumber(ARGV[2]) then
  return {0, "User limit reached"}
end

-- Still track checkouts for informational purposes
redis.call('INCR', KEYS[3])
redis.call('EXPIRE', KEYS[3], 3600) -- 1 hour expiration
return {1, "Success"}
`

// Redis Lua script for atomic purchase operations
// KEYS[1] = sale total key (sale:<saleID>:total)
// KEYS[2] = user purchases key (sale:<saleID>:user:<userID>:purchases)
// KEYS[3] = user checkouts key (sale:<saleID>:user:<userID>:checkouts)
// KEYS[4] = code key (code:<code>)
// ARGV[1] = MaxItemsPerSale
// ARGV[2] = MaxItemsPerUser
const PurchaseScript = `
local saleTotal = tonumber(redis.call('GET', KEYS[1]) or 0)
if saleTotal >= tonumber(ARGV[1]) then
  return {0, "Sale sold out"}
end

-- Check if user has reached their purchase limit
local userPurchases = tonumber(redis.call('GET', KEYS[2]) or 0)
if userPurchases >= tonumber(ARGV[2]) then
  return {0, "User purchase limit reached"}
end

-- Increment sale total counter
redis.call('INCR', KEYS[1])

-- Increment user purchases counter
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], 3600) -- 1 hour expiration

-- Decrement user checkouts counter
local checkouts = tonumber(redis.call('GET', KEYS[3]) or 0)
if checkouts > 0 then
  redis.call('DECR', KEYS[3])
end

-- Delete the code
redis.call('DEL', KEYS[4])

return {1, "Success"}
`

// RedisManager handles Redis operations
type RedisManager struct {
	Client *redis.Client
	Ctx    context.Context
}

// NewRedisManager creates a new RedisManager
func NewRedisManager(client *redis.Client, ctx context.Context) *RedisManager {
	return &RedisManager{
		Client: client,
		Ctx:    ctx,
	}
}

// ExecuteCheckoutScript executes the checkout script
func (rm *RedisManager) ExecuteCheckoutScript(saleID int, userID string) (bool, string, error) {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)

	// Execute the checkout script
	result, err := rm.Client.Eval(rm.Ctx, CheckoutScript, []string{
		saleKey,
		userPurchasesKey,
		userCheckoutsKey,
	}, models.MaxItemsPerSale, models.MaxItemsPerUser).Result()

	if err != nil {
		log.Printf("Error executing checkout script: %v", err)
		return false, "", err
	}

	// Check the result
	resultArray, ok := result.([]interface{})
	if !ok || len(resultArray) < 2 {
		log.Printf("Invalid result from checkout script: %v", result)
		return false, "", fmt.Errorf("invalid result from checkout script")
	}

	success, _ := resultArray[0].(int64)
	message, _ := resultArray[1].(string)

	return success == 1, message, nil
}

// ExecutePurchaseScript executes the purchase script
func (rm *RedisManager) ExecutePurchaseScript(saleID int, userID, code string) (bool, string, error) {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	codeKey := fmt.Sprintf("code:%s", code)

	// Execute the purchase script
	result, err := rm.Client.Eval(rm.Ctx, PurchaseScript, []string{
		saleKey,
		userPurchasesKey,
		userCheckoutsKey,
		codeKey,
	}, models.MaxItemsPerSale, models.MaxItemsPerUser).Result()

	if err != nil {
		log.Printf("Error executing purchase script: %v", err)
		return false, "", err
	}

	// Check the result
	resultArray, ok := result.([]interface{})
	if !ok || len(resultArray) < 2 {
		log.Printf("Invalid result from purchase script: %v", result)
		return false, "", fmt.Errorf("invalid result from purchase script")
	}

	success, _ := resultArray[0].(int64)
	message, _ := resultArray[1].(string)

	return success == 1, message, nil
}

// StoreCode stores a code in Redis with expiration
func (rm *RedisManager) StoreCode(code, userID, itemID string, saleID int) error {
	codeKey := fmt.Sprintf("code:%s", code)
	codeValue := fmt.Sprintf("%s:%s:%d", userID, itemID, saleID)
	return rm.Client.Set(rm.Ctx, codeKey, codeValue, models.CodeExpiration).Err()
}

// GetCodeValue gets the value of a code from Redis
func (rm *RedisManager) GetCodeValue(code string) (string, error) {
	codeKey := fmt.Sprintf("code:%s", code)
	return rm.Client.Get(rm.Ctx, codeKey).Result()
}

// SetSaleTotal sets the sale total in Redis
func (rm *RedisManager) SetSaleTotal(saleID, total int) error {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	return rm.Client.Set(rm.Ctx, saleKey, total, 0).Err()
}

// GetSaleTotal gets the sale total from Redis
func (rm *RedisManager) GetSaleTotal(saleID int) (int, error) {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	return rm.Client.Get(rm.Ctx, saleKey).Int()
}

// SetUserCheckouts sets the user checkouts in Redis
func (rm *RedisManager) SetUserCheckouts(saleID int, userID string, count int) error {
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	return rm.Client.Set(rm.Ctx, userCheckoutsKey, count, time.Hour).Err()
}

// GetUserCheckouts gets the user checkouts from Redis
func (rm *RedisManager) GetUserCheckouts(saleID int, userID string) (int, error) {
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	return rm.Client.Get(rm.Ctx, userCheckoutsKey).Int()
}

// SetUserPurchases sets the user purchases in Redis
func (rm *RedisManager) SetUserPurchases(saleID int, userID string, count int) error {
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	return rm.Client.Set(rm.Ctx, userPurchasesKey, count, time.Hour).Err()
}

// GetUserPurchases gets the user purchases from Redis
func (rm *RedisManager) GetUserPurchases(saleID int, userID string) (int, error) {
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	return rm.Client.Get(rm.Ctx, userPurchasesKey).Int()
}

// IncrementUserCheckouts increments the user checkouts in Redis
func (rm *RedisManager) IncrementUserCheckouts(saleID int, userID string) error {
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	_, err := rm.Client.Incr(rm.Ctx, userCheckoutsKey).Result()
	if err != nil {
		return err
	}
	return rm.Client.Expire(rm.Ctx, userCheckoutsKey, time.Hour).Err()
}

// DecrementUserCheckouts decrements the user checkouts in Redis
func (rm *RedisManager) DecrementUserCheckouts(saleID int, userID string) error {
	userCheckoutsKey := fmt.Sprintf("sale:%d:user:%s:checkouts", saleID, userID)
	return rm.Client.Decr(rm.Ctx, userCheckoutsKey).Err()
}

// IncrementUserPurchases increments the user purchases in Redis
func (rm *RedisManager) IncrementUserPurchases(saleID int, userID string) error {
	userPurchasesKey := fmt.Sprintf("sale:%d:user:%s:purchases", saleID, userID)
	_, err := rm.Client.Incr(rm.Ctx, userPurchasesKey).Result()
	if err != nil {
		return err
	}
	return rm.Client.Expire(rm.Ctx, userPurchasesKey, time.Hour).Err()
}

// IncrementSaleTotal increments the sale total in Redis
func (rm *RedisManager) IncrementSaleTotal(saleID int) error {
	saleKey := fmt.Sprintf("sale:%d:total", saleID)
	return rm.Client.Incr(rm.Ctx, saleKey).Err()
}

// DeleteCode deletes a code from Redis
func (rm *RedisManager) DeleteCode(code string) error {
	codeKey := fmt.Sprintf("code:%s", code)
	return rm.Client.Del(rm.Ctx, codeKey).Err()
}

// GetAllCodes gets all code keys from Redis
func (rm *RedisManager) GetAllCodes() ([]string, error) {
	return rm.Client.Keys(rm.Ctx, "code:*").Result()
}

// GetAllUserPurchaseKeys gets all user purchase keys for a sale from Redis
func (rm *RedisManager) GetAllUserPurchaseKeys(saleID int) ([]string, error) {
	return rm.Client.Keys(rm.Ctx, fmt.Sprintf("user:*:sale:%d:purchases", saleID)).Result()
}
