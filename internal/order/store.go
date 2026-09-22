package order

import (
	"context"
	"database/sql"
	"errors"
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
	Create(ctx context.Context, o Order) (Order, error)
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

func (s *PostgresOrderStore) Create(ctx context.Context, o Order) (Order, error) {
	const query = `
		INSERT INTO orders (id, user_id, asset_symbol, quantity, side, execution_type, limit_price, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING created_at
	`
	err := s.db.QueryRowContext(ctx, query,
		o.ID, o.UserID, o.AssetSymbol, o.Quantity, o.Side, o.ExecutionType, o.LimitPrice, o.Status,
	).Scan(&o.CreatedAt)
	if err != nil {
		return Order{}, fmt.Errorf("create order: %w", err)
	}
	return o, nil
}

// ErrOrderNotFound is returned by MarkExecuted when no order has the given id.
var ErrOrderNotFound = errors.New("order: not found")

// MarkExecuted moves an order from PENDING to EXECUTED in a single atomic
// statement. It reports whether this call performed the transition:
//
//   - (true, nil): the order was PENDING and is now EXECUTED.
//   - (false, nil): the order exists but was not PENDING (already executed,
//     cancelled or rejected). Nothing changed; the caller should skip it.
//   - (false, ErrOrderNotFound): no order has this id.
func (s *PostgresOrderStore) MarkExecuted(ctx context.Context, id uuid.UUID) (bool, error) {
	const updateQuery = `UPDATE orders SET status = $1 WHERE id = $2 AND status = $3`

	result, err := s.db.ExecContext(ctx, updateQuery, StatusExecuted, id, StatusPending)
	if err != nil {
		return false, fmt.Errorf("mark order executed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark order executed: rows affected: %w", err)
	}
	if rowsAffected == 1 {
		return true, nil
	}

	// 0 rows: the order is either not PENDING anymore or does not exist.
	const existsQuery = `SELECT EXISTS (SELECT 1 FROM orders WHERE id = $1)`

	var exists bool
	if err := s.db.QueryRowContext(ctx, existsQuery, id).Scan(&exists); err != nil {
		return false, fmt.Errorf("mark order executed: check existence: %w", err)
	}
	if !exists {
		return false, ErrOrderNotFound
	}
	return false, nil
}

// ExecuteOrder records the execution effect (an executions row) and moves
// the order from PENDING to EXECUTED, atomically. Same return semantics as
// the old MarkExecuted:
//   - (true, nil): the order was PENDING, now EXECUTED, execution recorded.
//   - (false, nil): the order was not PENDING — nothing changed, caller skips.
//   - (false, ErrOrderNotFound): no order has this id.
func (s *PostgresOrderStore) ExecuteOrder(ctx context.Context, orderID uuid.UUID, price float64, quantity int) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("execute order: begin transaction: %w", err)
	}
	defer tx.Rollback()

	const updateQuery = `UPDATE orders SET status = $1 WHERE id = $2 AND status = $3`

	result, err := tx.ExecContext(ctx, updateQuery, StatusExecuted, orderID, StatusPending)
	if err != nil {
		return false, fmt.Errorf("execute order: update status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("execute order: rows affected: %w", err)
	}

	if rowsAffected == 1 {
		// The transition happened — this call, and only this call, records
		// the effect. executed_at is left to the DB's DEFAULT now(), same
		// pattern as created_at in Create().
		const insertQuery = `
			INSERT INTO executions (id, order_id, execution_price, quantity)
			VALUES ($1, $2, $3, $4)
		`
		if _, err := tx.ExecContext(ctx, insertQuery, uuid.New(), orderID, price, quantity); err != nil {
			return false, fmt.Errorf("execute order: insert execution: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("execute order: commit: %w", err)
		}
		return true, nil
	}

	// 0 rows: the order is either not PENDING anymore or does not exist.
	const existsQuery = `SELECT EXISTS (SELECT 1 FROM orders WHERE id = $1)`

	var exists bool
	if err := tx.QueryRowContext(ctx, existsQuery, orderID).Scan(&exists); err != nil {
		return false, fmt.Errorf("execute order: check existence: %w", err)
	}
	if !exists {
		return false, ErrOrderNotFound
	}

	// Order exists but wasn't PENDING (duplicate delivery, or already
	// resolved another way). Nothing was written that needs to persist —
	// the deferred Rollback discards the failed UPDATE attempt.
	return false, nil
}
