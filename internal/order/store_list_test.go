package order

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"github.com/GabrielHKGodinho/investment-engine/internal/pagination"
)

// newMockStoreContains is like newMockStore, but matches expected queries as
// a plain substring of the real one. The List query is built with fmt.Sprintf
// and contains parentheses and $N placeholders, which are regexp metacharacters,
// so the library's default regexp matcher is the wrong tool here.
func newMockStoreContains(t *testing.T) (*PostgresOrderStore, sqlmock.Sqlmock) {
	t.Helper()

	matcher := sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
		if !strings.Contains(actualSQL, expectedSQL) {
			return errors.New("actual SQL does not contain expected SQL:\n actual:   " + actualSQL + "\n expected: " + expectedSQL)
		}
		return nil
	})

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
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

func TestPostgresOrderStore_List_Query(t *testing.T) {
	userID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	cursor := &pagination.Cursor{
		CreatedAt: time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC),
		ID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
	}
	after := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		filter    ListFilter
		wantWhere string         // substring the WHERE clause must contain
		wantArgs  []driver.Value // every argument, in the exact order List sends them
	}{
		{
			name:      "no filters uses the default limit",
			filter:    ListFilter{UserID: userID},
			wantWhere: "WHERE user_id = $1",
			wantArgs:  []driver.Value{userID.String(), int64(defaultLimit + 1)},
		},
		{
			name:      "cursor adds keyset pagination",
			filter:    ListFilter{UserID: userID, Limit: 10, Cursor: cursor},
			wantWhere: "user_id = $1 AND (created_at, id) < ($2, $3)",
			wantArgs:  []driver.Value{userID.String(), cursor.CreatedAt, cursor.ID.String(), int64(11)},
		},
		{
			name:      "status filter",
			filter:    ListFilter{UserID: userID, Limit: 10, Status: ptr(StatusExecuted)},
			wantWhere: "user_id = $1 AND status = $2",
			wantArgs:  []driver.Value{userID.String(), string(StatusExecuted), int64(11)},
		},
		{
			name:      "side filter",
			filter:    ListFilter{UserID: userID, Limit: 10, Side: ptr(SideSell)},
			wantWhere: "user_id = $1 AND side = $2",
			wantArgs:  []driver.Value{userID.String(), string(SideSell), int64(11)},
		},
		{
			name:      "executionType filter",
			filter:    ListFilter{UserID: userID, Limit: 10, ExecutionType: ptr(ExecutionLimit)},
			wantWhere: "user_id = $1 AND execution_type = $2",
			wantArgs:  []driver.Value{userID.String(), string(ExecutionLimit), int64(11)},
		},
		{
			name:      "assetSymbol filter",
			filter:    ListFilter{UserID: userID, Limit: 10, AssetSymbol: ptr("PETR4")},
			wantWhere: "user_id = $1 AND asset_symbol = $2",
			wantArgs:  []driver.Value{userID.String(), "PETR4", int64(11)},
		},
		{
			name:      "createdAfter filter",
			filter:    ListFilter{UserID: userID, Limit: 10, CreatedAfter: ptr(after)},
			wantWhere: "user_id = $1 AND created_at > $2",
			wantArgs:  []driver.Value{userID.String(), after, int64(11)},
		},
		{
			name:      "createdBefore filter",
			filter:    ListFilter{UserID: userID, Limit: 10, CreatedBefore: ptr(before)},
			wantWhere: "user_id = $1 AND created_at < $2",
			wantArgs:  []driver.Value{userID.String(), before, int64(11)},
		},
		{
			name: "every filter together, in the order the handler builds them",
			filter: ListFilter{
				UserID: userID, Limit: 10, Cursor: cursor,
				Status: ptr(StatusExecuted), Side: ptr(SideSell), ExecutionType: ptr(ExecutionLimit),
				AssetSymbol: ptr("PETR4"), CreatedAfter: ptr(after), CreatedBefore: ptr(before),
			},
			wantWhere: "user_id = $1 AND (created_at, id) < ($2, $3) AND status = $4 AND side = $5 " +
				"AND execution_type = $6 AND asset_symbol = $7 AND created_at > $8 AND created_at < $9",
			wantArgs: []driver.Value{
				userID.String(), cursor.CreatedAt, cursor.ID.String(),
				string(StatusExecuted), string(SideSell), string(ExecutionLimit),
				"PETR4", after, before, int64(11),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, mock := newMockStoreContains(t)

			cols := []string{"id", "user_id", "asset_symbol", "quantity", "side", "execution_type", "limit_price", "status", "created_at"}
			mock.ExpectQuery(tt.wantWhere).
				WithArgs(tt.wantArgs...).
				WillReturnRows(sqlmock.NewRows(cols)) // no rows: this test only checks the query, not the mapping

			_, _, err := store.List(context.Background(), tt.filter)
			if err != nil {
				t.Fatalf("List() error = %v; the query sent did not match %q with args %v", err, tt.wantWhere, tt.wantArgs)
			}
		})
	}
}

func TestPostgresOrderStore_List_Rows(t *testing.T) {
	cols := []string{"id", "user_id", "asset_symbol", "quantity", "side", "execution_type", "limit_price", "status", "created_at"}
	userID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	row := func(id string, createdAt time.Time) []driver.Value {
		return []driver.Value{
			id, userID.String(), "PETR4", int64(10), string(SideBuy), string(ExecutionMarket), nil, string(StatusPending), createdAt,
		}
	}

	t1 := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(-time.Hour)
	t3 := t2.Add(-time.Hour)

	tests := []struct {
		name        string
		limit       int
		dbRows      [][]driver.Value // what the fake database returns, LIMIT+1 already applied by the caller
		wantCount   int
		wantHasMore bool
	}{
		{
			name:        "fewer rows than the limit: last page",
			limit:       10,
			dbRows:      [][]driver.Value{row("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", t1)},
			wantCount:   1,
			wantHasMore: false,
		},
		{
			name:  "exactly the limit: also the last page",
			limit: 2,
			dbRows: [][]driver.Value{
				row("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", t1),
				row("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", t2),
			},
			wantCount:   2,
			wantHasMore: false,
		},
		{
			name:  "one more row than the limit: there is a next page",
			limit: 2,
			dbRows: [][]driver.Value{
				row("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", t1),
				row("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", t2),
				row("cccccccc-cccc-cccc-cccc-cccccccccccc", t3), // the extra row: trimmed, never returned to the caller
			},
			wantCount:   2,
			wantHasMore: true,
		},
		{
			name:        "no rows at all",
			limit:       10,
			dbRows:      nil,
			wantCount:   0,
			wantHasMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, mock := newMockStoreContains(t)

			rows := sqlmock.NewRows(cols)
			for _, r := range tt.dbRows {
				rows.AddRow(r...)
			}
			mock.ExpectQuery("").WillReturnRows(rows)

			gotOrders, gotCursor, err := store.List(context.Background(), ListFilter{UserID: userID, Limit: tt.limit})
			if err != nil {
				t.Fatalf("List() error = %v", err)
			}

			if len(gotOrders) != tt.wantCount {
				t.Fatalf("got %d orders, want %d", len(gotOrders), tt.wantCount)
			}
			if (gotCursor != nil) != tt.wantHasMore {
				t.Errorf("nextCursor = %v, want hasMore = %t", gotCursor, tt.wantHasMore)
			}
			if tt.wantHasMore {
				lastReturned := gotOrders[len(gotOrders)-1]
				if gotCursor.ID != lastReturned.ID || !gotCursor.CreatedAt.Equal(lastReturned.CreatedAt) {
					t.Errorf("nextCursor = %+v, want it to match the last returned order %+v", gotCursor, lastReturned)
				}
			}
		})
	}

	t.Run("database error is wrapped", func(t *testing.T) {
		t.Parallel()
		store, mock := newMockStoreContains(t)
		mock.ExpectQuery("").WillReturnError(errors.New("connection reset by peer"))

		_, _, err := store.List(context.Background(), ListFilter{UserID: userID})
		if err == nil || !strings.Contains(err.Error(), "connection reset by peer") {
			t.Errorf("List() error = %v, want it to wrap the database error", err)
		}
	})
}
