package order

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// newMockStore returns a store backed by a scripted fake database. Each test
// writes its script on the returned mock. When the test ends, the cleanup
// checks that every scripted step really happened.
func newMockStore(t *testing.T) (*PostgresOrderStore, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet database expectations: %v", err)
		}
		db.Close()
	})

	return NewPostgresOrderStore(db), mock
}

func TestPostgresOrderStore_Create(t *testing.T) {
	createdAt := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

	marketOrder := Order{
		ID:            uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
		UserID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		AssetSymbol:   "PETR4",
		Quantity:      10,
		Side:          SideBuy,
		ExecutionType: ExecutionMarket,
		Status:        StatusPending,
	}
	limitOrder := marketOrder
	limitOrder.ExecutionType = ExecutionLimit
	limitOrder.LimitPrice = ptr(61.5)

	tests := []struct {
		name  string
		order Order
		dbErr error // when set, the fake database fails the INSERT
	}{
		{name: "market order has no limit price", order: marketOrder},
		{name: "limit order sends its limit price", order: limitOrder},
		{name: "database error is wrapped and returned", order: marketOrder, dbErr: errors.New("connection reset by peer")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, mock := newMockStore(t)

			// The script: "expect an INSERT INTO orders with these 8 arguments, in this order".
			expected := mock.ExpectQuery("INSERT INTO orders").WithArgs(
				tt.order.ID, tt.order.UserID, tt.order.AssetSymbol, tt.order.Quantity,
				tt.order.Side, tt.order.ExecutionType, tt.order.LimitPrice, tt.order.Status,
			)
			if tt.dbErr != nil {
				expected.WillReturnError(tt.dbErr)
			} else {
				expected.WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(createdAt))
			}

			got, err := store.Create(context.Background(), tt.order)

			if tt.dbErr != nil {
				if !errors.Is(err, tt.dbErr) {
					t.Fatalf("Create() error = %v, want it to wrap %v", err, tt.dbErr)
				}
				if !reflect.DeepEqual(got, Order{}) {
					t.Errorf("Create() returned %+v on error, want the zero Order", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			want := tt.order
			want.CreatedAt = createdAt // the only field the database fills in
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Create() = %+v, want %+v", got, want)
			}
		})
	}
}
