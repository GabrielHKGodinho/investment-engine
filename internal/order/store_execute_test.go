package order

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestPostgresOrderStore_ExecuteOrder(t *testing.T) {
	orderID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	const price = 38.42
	const quantity = 10

	t.Run("order was PENDING: executes and commits", func(t *testing.T) {
		t.Parallel()
		store, mock := newMockStoreContains(t)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE orders SET status").
			WithArgs(string(StatusExecuted), orderID.String(), string(StatusPending)).
			WillReturnResult(sqlmock.NewResult(0, 1)) // 1 row affected: the transition happened
		mock.ExpectExec("INSERT INTO executions").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		got, err := store.ExecuteOrder(context.Background(), orderID, price, quantity)
		if err != nil {
			t.Fatalf("ExecuteOrder() error = %v", err)
		}
		if !got {
			t.Errorf("ExecuteOrder() = %t, want true", got)
		}
	})

	t.Run("order exists but was not PENDING: no-op, rolls back", func(t *testing.T) {
		t.Parallel()
		store, mock := newMockStoreContains(t)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE orders SET status").
			WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows: WHERE status = 'PENDING' did not match
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(orderID.String()).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectRollback()

		got, err := store.ExecuteOrder(context.Background(), orderID, price, quantity)
		if err != nil {
			t.Fatalf("ExecuteOrder() error = %v", err)
		}
		if got {
			t.Errorf("ExecuteOrder() = %t, want false", got)
		}
	})

	t.Run("order does not exist: returns ErrOrderNotFound, rolls back", func(t *testing.T) {
		t.Parallel()
		store, mock := newMockStoreContains(t)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE orders SET status").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("SELECT EXISTS").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectRollback()

		got, err := store.ExecuteOrder(context.Background(), orderID, price, quantity)
		if !errors.Is(err, ErrOrderNotFound) {
			t.Fatalf("ExecuteOrder() error = %v, want ErrOrderNotFound", err)
		}
		if got {
			t.Errorf("ExecuteOrder() = %t, want false", got)
		}
	})

	t.Run("BeginTx fails: no rollback to attempt", func(t *testing.T) {
		t.Parallel()
		store, mock := newMockStoreContains(t)

		mock.ExpectBegin().WillReturnError(errors.New("connection refused"))

		_, err := store.ExecuteOrder(context.Background(), orderID, price, quantity)
		if err == nil {
			t.Fatal("ExecuteOrder() returned no error, want one")
		}
	})

	t.Run("UPDATE fails mid-transaction: rolls back", func(t *testing.T) {
		t.Parallel()
		store, mock := newMockStoreContains(t)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE orders SET status").
			WillReturnError(errors.New("connection reset by peer"))
		mock.ExpectRollback()

		_, err := store.ExecuteOrder(context.Background(), orderID, price, quantity)
		if err == nil {
			t.Fatal("ExecuteOrder() returned no error, want one")
		}
	})

	t.Run("execution insert fails: does not commit, rolls back", func(t *testing.T) {
		t.Parallel()
		store, mock := newMockStoreContains(t)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE orders SET status").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO executions").
			WillReturnError(errors.New("unique constraint violation"))
		mock.ExpectRollback()

		got, err := store.ExecuteOrder(context.Background(), orderID, price, quantity)
		if err == nil {
			t.Fatal("ExecuteOrder() returned no error, want one")
		}
		if got {
			t.Errorf("ExecuteOrder() = %t, want false", got)
		}
	})

	t.Run("commit fails", func(t *testing.T) {
		t.Parallel()
		store, mock := newMockStoreContains(t)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE orders SET status").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO executions").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("connection reset by peer"))

		got, err := store.ExecuteOrder(context.Background(), orderID, price, quantity)
		if err == nil {
			t.Fatal("ExecuteOrder() returned no error, want one")
		}
		if got {
			t.Errorf("ExecuteOrder() = %t, want false", got)
		}
	})
}
