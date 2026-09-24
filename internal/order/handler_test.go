package order

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GabrielHKGodinho/investment-engine/internal/apierror"
	"github.com/GabrielHKGodinho/investment-engine/internal/auth"
	"github.com/GabrielHKGodinho/investment-engine/internal/pagination"
)

// errorEnvelope mirrors the JSON error contract on purpose instead of reusing
// apierror's unexported types: the test checks what goes over the wire, not
// how the implementation happens to name things.
type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Fields  []struct {
			Field   string `json:"field"`
			Message string `json:"message"`
		} `json:"fields"`
	} `json:"error"`
}

// callListOrders runs the handler against an in-memory request. No network,
// no server: httptest.ResponseRecorder captures what the handler wrote.
func callListOrders(t *testing.T, store OrderStore, query url.Values) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewHandler(store, nil)
	req := httptest.NewRequest(http.MethodGet, "/orders?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	handler.ListOrders(rec, req)
	return rec
}

func ptr[T any](v T) *T { return &v }

func TestListOrders_Filter(t *testing.T) {
	userID, err := auth.UserIDFromContext(t.Context())
	if err != nil {
		t.Fatalf("failed to get the simulated user: %v", err)
	}

	cursor := pagination.Cursor{
		CreatedAt: time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC),
		ID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
	}
	token, err := pagination.EncodeCursor(cursor)
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}

	tests := []struct {
		name  string
		query url.Values
		want  ListFilter // what the handler must ask the store for
	}{
		{
			name:  "no params uses defaults",
			query: url.Values{},
			want:  ListFilter{UserID: userID, Limit: 20},
		},
		{
			name:  "explicit limit",
			query: url.Values{"limit": {"5"}},
			want:  ListFilter{UserID: userID, Limit: 5},
		},
		{
			name:  "limit at the minimum",
			query: url.Values{"limit": {"1"}},
			want:  ListFilter{UserID: userID, Limit: 1},
		},
		{
			name:  "limit at the maximum",
			query: url.Values{"limit": {"100"}},
			want:  ListFilter{UserID: userID, Limit: 100},
		},
		{
			name:  "limit above the maximum is clamped",
			query: url.Values{"limit": {"101"}},
			want:  ListFilter{UserID: userID, Limit: 100},
		},
		{
			name:  "enum params are case-insensitive",
			query: url.Values{"status": {"pending"}, "side": {"buy"}, "executionType": {"limit"}},
			want: ListFilter{
				UserID:        userID,
				Limit:         20,
				Status:        ptr(StatusPending),
				Side:          ptr(SideBuy),
				ExecutionType: ptr(ExecutionLimit),
			},
		},
		{
			name: "all filters together",
			query: url.Values{
				"limit":         {"10"},
				"cursor":        {token},
				"status":        {"EXECUTED"},
				"assetSymbol":   {"PETR4"},
				"createdAfter":  {"2026-01-01T00:00:00Z"},
				"createdBefore": {"2026-12-31T00:00:00Z"},
			},
			want: ListFilter{
				UserID:        userID,
				Limit:         10,
				Cursor:        &cursor,
				Status:        ptr(StatusExecuted),
				AssetSymbol:   ptr("PETR4"),
				CreatedAfter:  ptr(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
				CreatedBefore: ptr(time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeOrderStore{}

			rec := callListOrders(t, store, tt.query)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			if store.listCalls != 1 {
				t.Fatalf("store.List called %d times, want 1", store.listCalls)
			}
			if !reflect.DeepEqual(store.gotFilter, tt.want) {
				t.Errorf("filter passed to store = %+v, want %+v", store.gotFilter, tt.want)
			}
		})
	}
}

func TestListOrders_Errors(t *testing.T) {
	emptyObjectCursor := base64.URLEncoding.EncodeToString([]byte(`{}`))

	tests := []struct {
		name       string
		query      url.Values
		storeErr   error
		wantStatus int
		wantCode   string
		wantFields []string // names of the fields reported as invalid, in any order
	}{
		{name: "limit zero", query: url.Values{"limit": {"0"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"limit"}},
		{name: "limit negative", query: url.Values{"limit": {"-1"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"limit"}},
		{name: "limit not a number", query: url.Values{"limit": {"abc"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"limit"}},
		{name: "unknown status", query: url.Values{"status": {"BOGUS"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"status"}},
		{name: "unknown side", query: url.Values{"side": {"HOLD"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"side"}},
		{name: "unknown executionType", query: url.Values{"executionType": {"STOP"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"executionType"}},
		{name: "createdAfter not RFC3339", query: url.Values{"createdAfter": {"yesterday"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"createdAfter"}},
		{name: "createdBefore not RFC3339", query: url.Values{"createdBefore": {"2026-13-45"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"createdBefore"}},
		{name: "cursor is garbage", query: url.Values{"cursor": {"garbage!!"}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"cursor"}},
		{name: "cursor is well-formed but carries no position", query: url.Values{"cursor": {emptyObjectCursor}}, wantStatus: 400, wantCode: apierror.CodeValidation, wantFields: []string{"cursor"}},
		{
			name:       "several invalid params are all reported at once",
			query:      url.Values{"limit": {"abc"}, "side": {"HOLD"}, "status": {"BOGUS"}},
			wantStatus: 400,
			wantCode:   apierror.CodeValidation,
			wantFields: []string{"limit", "side", "status"},
		},
		{
			name:       "store failure becomes a generic 500",
			query:      url.Values{},
			storeErr:   errors.New("dial tcp 10.0.0.5:5432: connection refused"),
			wantStatus: 500,
			wantCode:   apierror.CodeInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeOrderStore{listErr: tt.storeErr}

			rec := callListOrders(t, store, tt.query)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}

			var body errorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("error body is not the expected JSON envelope: %v; body: %s", err, rec.Body.String())
			}
			if body.Error.Code != tt.wantCode {
				t.Errorf("error code = %q, want %q", body.Error.Code, tt.wantCode)
			}

			var gotFields []string
			for _, f := range body.Error.Fields {
				gotFields = append(gotFields, f.Field)
			}
			slices.Sort(gotFields)
			slices.Sort(tt.wantFields)
			if !slices.Equal(gotFields, tt.wantFields) {
				t.Errorf("invalid fields = %v, want %v", gotFields, tt.wantFields)
			}

			if tt.wantStatus == http.StatusBadRequest && store.listCalls != 0 {
				t.Errorf("store.List called %d times for an invalid request, want 0", store.listCalls)
			}
			if tt.storeErr != nil && strings.Contains(rec.Body.String(), "connection refused") {
				t.Errorf("response leaks the internal error: %s", rec.Body.String())
			}
		})
	}
}

func TestListOrders_Response(t *testing.T) {
	first := Order{
		ID:            uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
		AssetSymbol:   "PETR4",
		Quantity:      10,
		Side:          SideBuy,
		ExecutionType: ExecutionMarket,
		Status:        StatusPending,
		CreatedAt:     time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC),
	}
	second := Order{
		ID:            uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
		AssetSymbol:   "VALE3",
		Quantity:      5,
		Side:          SideSell,
		ExecutionType: ExecutionLimit,
		LimitPrice:    ptr(61.5),
		Status:        StatusExecuted,
		CreatedAt:     time.Date(2026, 9, 21, 11, 0, 0, 0, time.UTC),
	}
	next := pagination.Cursor{CreatedAt: second.CreatedAt, ID: second.ID}

	tests := []struct {
		name        string
		orders      []Order
		next        *pagination.Cursor
		wantIDs     []string
		wantHasMore bool
	}{
		{
			name:        "page with more results",
			orders:      []Order{first, second},
			next:        &next,
			wantIDs:     []string{first.ID.String(), second.ID.String()},
			wantHasMore: true,
		},
		{
			name:        "last page",
			orders:      []Order{first},
			next:        nil,
			wantIDs:     []string{first.ID.String()},
			wantHasMore: false,
		},
		{
			name:        "no orders",
			orders:      nil, // the real store returns a nil slice when nothing matches
			next:        nil,
			wantIDs:     nil,
			wantHasMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeOrderStore{listOrders: tt.orders, listNext: tt.next}

			rec := callListOrders(t, store, url.Values{})

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
			}
			var got ListOrdersResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("response is not valid JSON: %v", err)
			}

			var gotIDs []string
			for _, o := range got.Orders {
				gotIDs = append(gotIDs, o.OrderID)
			}
			if !slices.Equal(gotIDs, tt.wantIDs) {
				t.Errorf("order IDs = %v, want %v", gotIDs, tt.wantIDs)
			}
			if got.HasMore != tt.wantHasMore {
				t.Errorf("hasMore = %t, want %t", got.HasMore, tt.wantHasMore)
			}

			if tt.next == nil {
				if got.NextCursor != nil {
					t.Errorf("nextCursor = %q, want null", *got.NextCursor)
				}
			} else {
				if got.NextCursor == nil {
					t.Fatal("nextCursor is null, want a token")
				}
				decoded, err := pagination.DecodeCursor(*got.NextCursor)
				if err != nil {
					t.Fatalf("nextCursor is not decodable: %v", err)
				}
				if decoded.ID != tt.next.ID || !decoded.CreatedAt.Equal(tt.next.CreatedAt) {
					t.Errorf("nextCursor decodes to %+v, want %+v", decoded, *tt.next)
				}
			}
		})
	}

	t.Run("empty result is an empty array, never null", func(t *testing.T) {
		t.Parallel()
		rec := callListOrders(t, &fakeOrderStore{}, url.Values{})
		if !strings.Contains(rec.Body.String(), `"orders":[]`) {
			t.Errorf(`body = %s, want it to contain "orders":[]`, rec.Body.String())
		}
	})

	t.Run("limit price is omitted for market orders and present for limit orders", func(t *testing.T) {
		t.Parallel()
		store := &fakeOrderStore{listOrders: []Order{first, second}}
		rec := callListOrders(t, store, url.Values{})

		var got ListOrdersResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("response is not valid JSON: %v", err)
		}
		if got.Orders[0].LimitPrice != nil {
			t.Errorf("market order limitPrice = %v, want omitted", *got.Orders[0].LimitPrice)
		}
		if got.Orders[1].LimitPrice == nil || *got.Orders[1].LimitPrice != 61.5 {
			t.Errorf("limit order limitPrice = %v, want 61.5", got.Orders[1].LimitPrice)
		}
	})
}

func TestCreateOrder_Success(t *testing.T) {
	store := &fakeOrderStore{}
	publisher := &fakePublisher{}
	handler := NewHandler(store, publisher)

	body := `{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"MARKET"}`
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.CreateOrder(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	// The order was saved exactly once, as PENDING, for the authenticated user.
	if store.createCalls != 1 {
		t.Fatalf("store.Create called %d times, want 1", store.createCalls)
	}
	wantUser, _ := auth.UserIDFromContext(req.Context())
	if store.gotCreated.UserID != wantUser {
		t.Errorf("saved UserID = %s, want %s", store.gotCreated.UserID, wantUser)
	}
	if store.gotCreated.Status != StatusPending {
		t.Errorf("saved Status = %s, want %s", store.gotCreated.Status, StatusPending)
	}

	// The event was published exactly once, about the order that was saved.
	if publisher.calls != 1 {
		t.Fatalf("publisher called %d times, want 1", publisher.calls)
	}
	if publisher.got.OrderID != store.gotCreated.ID {
		t.Errorf("event OrderID = %s, want %s", publisher.got.OrderID, store.gotCreated.ID)
	}

	// The response tells the client which order was created.
	var resp OrderResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if resp.OrderID != store.gotCreated.ID.String() {
		t.Errorf("response orderID = %s, want %s", resp.OrderID, store.gotCreated.ID)
	}
}

func TestCreateOrder_Validation(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantFields []string // names of the fields reported as invalid, in any order
	}{
		{
			name: "malformed JSON",
			body: `{not json`,
			// no wantFields: the whole body is unreadable, not one field
		},
		{
			name:       "empty object reports every required field",
			body:       `{}`,
			wantFields: []string{"assetSymbol", "quantity", "side", "executionType"},
		},
		{
			name:       "blank asset symbol",
			body:       `{"assetSymbol":"   ","quantity":10,"side":"BUY","executionType":"MARKET"}`,
			wantFields: []string{"assetSymbol"},
		},
		{
			name:       "quantity zero",
			body:       `{"assetSymbol":"PETR4","quantity":0,"side":"BUY","executionType":"MARKET"}`,
			wantFields: []string{"quantity"},
		},
		{
			name:       "quantity negative",
			body:       `{"assetSymbol":"PETR4","quantity":-5,"side":"BUY","executionType":"MARKET"}`,
			wantFields: []string{"quantity"},
		},
		{
			name:       "unknown side",
			body:       `{"assetSymbol":"PETR4","quantity":10,"side":"HOLD","executionType":"MARKET"}`,
			wantFields: []string{"side"},
		},
		{
			name:       "unknown executionType",
			body:       `{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"STOP"}`,
			wantFields: []string{"executionType"},
		},
		{
			name:       "LIMIT order without limitPrice",
			body:       `{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"LIMIT"}`,
			wantFields: []string{"limitPrice"},
		},
		{
			name:       "LIMIT order with zero limitPrice",
			body:       `{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"LIMIT","limitPrice":0}`,
			wantFields: []string{"limitPrice"},
		},
		{
			name:       "MARKET order with limitPrice",
			body:       `{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"MARKET","limitPrice":38.5}`,
			wantFields: []string{"limitPrice"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeOrderStore{}
			publisher := &fakePublisher{}
			handler := NewHandler(store, publisher)

			req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			handler.CreateOrder(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			var body errorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("error body is not the expected JSON envelope: %v; body: %s", err, rec.Body.String())
			}
			if body.Error.Code != apierror.CodeValidation {
				t.Errorf("error code = %q, want %q", body.Error.Code, apierror.CodeValidation)
			}

			var gotFields []string
			for _, f := range body.Error.Fields {
				gotFields = append(gotFields, f.Field)
			}
			slices.Sort(gotFields)
			slices.Sort(tt.wantFields)
			if !slices.Equal(gotFields, tt.wantFields) {
				t.Errorf("invalid fields = %v, want %v", gotFields, tt.wantFields)
			}

			// An invalid request must not save anything nor publish anything.
			if store.createCalls != 0 {
				t.Errorf("store.Create called %d times, want 0", store.createCalls)
			}
			if publisher.calls != 0 {
				t.Errorf("publisher called %d times, want 0", publisher.calls)
			}
		})
	}
}

func TestCreateOrder_SavedOrder(t *testing.T) {
	wantUser, err := auth.UserIDFromContext(t.Context())
	if err != nil {
		t.Fatalf("failed to get the simulated user: %v", err)
	}

	tests := []struct {
		name string
		body string
		want Order // ID, UserID and Status are filled in by the loop below
	}{
		{
			name: "enum values are normalized to upper case",
			body: `{"assetSymbol":"PETR4","quantity":10,"side":"buy","executionType":"market"}`,
			want: Order{AssetSymbol: "PETR4", Quantity: 10, Side: SideBuy, ExecutionType: ExecutionMarket},
		},
		{
			name: "asset symbol is trimmed and upper-cased",
			body: `{"assetSymbol":"  petr4 ","quantity":10,"side":"BUY","executionType":"MARKET"}`,
			want: Order{AssetSymbol: "PETR4", Quantity: 10, Side: SideBuy, ExecutionType: ExecutionMarket},
		},
		{
			name: "LIMIT order keeps its limit price",
			body: `{"assetSymbol":"VALE3","quantity":5,"side":"SELL","executionType":"LIMIT","limitPrice":61.5}`,
			want: Order{AssetSymbol: "VALE3", Quantity: 5, Side: SideSell, ExecutionType: ExecutionLimit, LimitPrice: ptr(61.5)},
		},
		{
			name: "userID sent in the body is ignored",
			body: `{"userID":"22222222-2222-2222-2222-222222222222","assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"MARKET"}`,
			want: Order{AssetSymbol: "PETR4", Quantity: 10, Side: SideBuy, ExecutionType: ExecutionMarket},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeOrderStore{}
			handler := NewHandler(store, &fakePublisher{})

			req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			handler.CreateOrder(rec, req)

			if rec.Code != http.StatusCreated {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
			}

			got := store.gotCreated
			got.ID = uuid.Nil // random on every call, so it is not part of what this test checks

			want := tt.want
			want.UserID = wantUser // always the authenticated user, never the one from the body
			want.Status = StatusPending

			if !reflect.DeepEqual(got, want) {
				t.Errorf("saved order = %+v, want %+v", got, want)
			}
		})
	}
}

func TestCreateOrder_DependencyFailures(t *testing.T) {
	const validBody = `{"assetSymbol":"PETR4","quantity":10,"side":"BUY","executionType":"MARKET"}`

	tests := []struct {
		name               string
		storeErr           error
		publishErr         error
		wantStatus         int
		wantCode           string // empty when the request succeeds
		wantPublisherCalls int
	}{
		{
			name:               "store failure becomes a generic 500 and nothing is published",
			storeErr:           errors.New("dial tcp 10.0.0.5:5432: connection refused"),
			wantStatus:         http.StatusInternalServerError,
			wantCode:           apierror.CodeInternal,
			wantPublisherCalls: 0,
		},
		{
			// Known limitation (ADR-007): the order is already saved, so the
			// client still gets 201 even though the event was lost.
			name:               "publish failure does not fail the request",
			publishErr:         errors.New("channel/connection is not open"),
			wantStatus:         http.StatusCreated,
			wantPublisherCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeOrderStore{createErr: tt.storeErr}
			publisher := &fakePublisher{err: tt.publishErr}
			handler := NewHandler(store, publisher)

			req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(validBody))
			rec := httptest.NewRecorder()

			handler.CreateOrder(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if store.createCalls != 1 {
				t.Errorf("store.Create called %d times, want 1", store.createCalls)
			}
			if publisher.calls != tt.wantPublisherCalls {
				t.Errorf("publisher called %d times, want %d", publisher.calls, tt.wantPublisherCalls)
			}

			if tt.wantCode != "" {
				var body errorEnvelope
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("error body is not the expected JSON envelope: %v; body: %s", err, rec.Body.String())
				}
				if body.Error.Code != tt.wantCode {
					t.Errorf("error code = %q, want %q", body.Error.Code, tt.wantCode)
				}
			}

			// The text of an internal error must never reach the client.
			for _, internal := range []error{tt.storeErr, tt.publishErr} {
				if internal != nil && strings.Contains(rec.Body.String(), internal.Error()) {
					t.Errorf("response leaks the internal error %q: %s", internal, rec.Body.String())
				}
			}
		})
	}
}
