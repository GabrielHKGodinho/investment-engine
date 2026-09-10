package order

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GabrielHKGodinho/investment-engine/internal/pagination"
)

// ListFilter holds the optional filters and pagination params for List.
// UserID is always required and comes from the authenticated caller — never from client input.
type ListFilter struct {
	UserID        uuid.UUID
	Cursor        *pagination.Cursor // nil means "start from the beginning"
	Limit         int
	Status        *Status
	Side          *Side
	ExecutionType *ExecutionType
	AssetSymbol   *string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
}

type OrderStore interface {
	List(ctx context.Context, filter ListFilter) (orders []Order, nextCursor *pagination.Cursor, err error)
}

type PostgresOrderStore struct {
	db *sql.DB
}

func NewPostgresOrderStore(db *sql.DB) *PostgresOrderStore {
	return &PostgresOrderStore{db: db}
}

func (s *PostgresOrderStore) List(ctx context.Context, filter ListFilter) ([]Order, *pagination.Cursor, error) {
	if filter.Limit <= 0 {
		filter.Limit = defaultLimit
	}
	conditions := []string{"user_id = $1"}
	args := []any{filter.UserID}

	if filter.Cursor != nil {
		args = append(args, filter.Cursor.CreatedAt, filter.Cursor.ID)
		conditions = append(conditions, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	if filter.Status != nil {
		args = append(args, *filter.Status)
		conditions = append(conditions, fmt.Sprintf("status = $%d", len(args)))
	}
	if filter.Side != nil {
		args = append(args, *filter.Side)
		conditions = append(conditions, fmt.Sprintf("side = $%d", len(args)))
	}
	if filter.ExecutionType != nil {
		args = append(args, *filter.ExecutionType)
		conditions = append(conditions, fmt.Sprintf("execution_type = $%d", len(args)))
	}
	if filter.AssetSymbol != nil {
		args = append(args, *filter.AssetSymbol)
		conditions = append(conditions, fmt.Sprintf("asset_symbol = $%d", len(args)))
	}
	if filter.CreatedAfter != nil {
		args = append(args, *filter.CreatedAfter)
		conditions = append(conditions, fmt.Sprintf("created_at > $%d", len(args)))
	}
	if filter.CreatedBefore != nil {
		args = append(args, *filter.CreatedBefore)
		conditions = append(conditions, fmt.Sprintf("created_at < $%d", len(args)))
	}

	// Ask for one row more than needed — its presence tells us hasMore, no extra COUNT query.
	args = append(args, filter.Limit+1)

	query := fmt.Sprintf(`
		SELECT id, user_id, asset_symbol, quantity, side, execution_type, limit_price, status, created_at
		FROM orders
		WHERE %s
		ORDER BY created_at DESC, id DESC
		LIMIT $%d
	`, strings.Join(conditions, " AND "), len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list orders: query: %w", err)
	}
	defer rows.Close()

	var orders []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.AssetSymbol, &o.Quantity, &o.Side, &o.ExecutionType, &o.LimitPrice, &o.Status, &o.CreatedAt); err != nil {
			return nil, nil, fmt.Errorf("list orders: scan: %w", err)
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("list orders: rows: %w", err)
	}

	var nextCursor *pagination.Cursor
	if len(orders) > filter.Limit {
		orders = orders[:filter.Limit]
		last := orders[len(orders)-1]
		nextCursor = &pagination.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}

	return orders, nextCursor, nil
}
