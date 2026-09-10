package order

import (
	"time"

	"github.com/google/uuid"
)

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type ExecutionType string

const (
	ExecutionMarket ExecutionType = "MARKET"
	ExecutionLimit  ExecutionType = "LIMIT"
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusExecuted  Status = "EXECUTED"
	StatusCancelled Status = "CANCELLED"
	StatusRejected  Status = "REJECTED"
)

type Order struct {
	ID            uuid.UUID
	UserID        uuid.UUID
	AssetSymbol   string
	Quantity      int
	Side          Side
	ExecutionType ExecutionType
	LimitPrice    *float64
	Status        Status
	CreatedAt     time.Time
}

func isValidStatus(s Status) bool {
	switch s {
	case StatusPending, StatusExecuted, StatusCancelled, StatusRejected:
		return true
	}
	return false
}

func isValidSide(s Side) bool {
	return s == SideBuy || s == SideSell
}

func isValidExecutionType(e ExecutionType) bool {
	return e == ExecutionMarket || e == ExecutionLimit
}
