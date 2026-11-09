package strategy

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

type strategyContextKey string

const StrategyOverrideKey strategyContextKey = "strategy_override"

type AdaptiveStrategy struct {
	pessimistic         *PessimisticStrategy
	optimistic          *OptimisticStrategy
	atomic              *AtomicStrategy
	rdb                 *redis.Client
	contentionThreshold int64
}

func NewAdaptiveStrategy(rdb *redis.Client) *AdaptiveStrategy {
	return &AdaptiveStrategy{
		pessimistic:         NewPessimisticStrategy(),
		optimistic:          NewOptimisticStrategy(),
		atomic:              NewAtomicStrategy(),
		rdb:                 rdb,
		contentionThreshold: 10,
	}
}

func (s *AdaptiveStrategy) Name() string{
	return "adaptive"
}

func (s *AdaptiveStrategy) recordAndCheckContention(ctx context.Context, items []ReservationItem) bool {
	if s.rdb == nil{
		return false
	}

	isHigh:= false
	for _, item := range items{
		rateKey := fmt.Sprintf("rate:sku:%s", item.SKU)

		pipe := s.rdb.Pipeline()
		incrCmd := pipe.Incr(ctx, rateKey)
		pipe.Expire(ctx, rateKey, 1*time.Second)
		_, err := pipe.Exec(ctx)

		if err ==nil && incrCmd.Val()>= s.contentionThreshold {
			isHigh =true
		}
	}

	return isHigh
}

func (s *AdaptiveStrategy)Reserve(ctx context.Context, qtx *db.Queries, items []ReservationItem) error{
	if override, ok := ctx.Value(StrategyOverrideKey).(string); ok {
		switch override{
		case "pessimistic":
			return s.pessimistic.Reserve(ctx, qtx, items)
		case "optimistic":
			return s.optimistic.Reserve(ctx, qtx, items)
		case "atomic":
			return s.atomic.Reserve(ctx, qtx, items)
		}
	}

	if len(items) == 1{
		return s.atomic.Reserve(ctx, qtx, items)
	}

	isHighContention := s.recordAndCheckContention(ctx, items)
	if isHighContention{
		return s.pessimistic.Reserve(ctx, qtx, items)
	}

	return s.optimistic.Reserve(ctx, qtx, items)
}