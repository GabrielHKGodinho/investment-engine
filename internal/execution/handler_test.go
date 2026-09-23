package execution

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
)

type fakeExecutionStore struct {
	executed bool
	err      error

	called   bool
	gotID    uuid.UUID
	gotPrice float64
	gotQty   int
}

func (f *fakeExecutionStore) ExecuteOrder(ctx context.Context, id uuid.UUID, price float64, quantity int) (bool, error) {
	f.called = true
	f.gotID = id
	f.gotPrice = price
	f.gotQty = quantity
	return f.executed, f.err
}

type fakePriceGetter struct {
	price float64
	err   error
}

func (f *fakePriceGetter) GetPrice(ctx context.Context, symbol string) (float64, error) {
	return f.price, f.err
}

// validEventJSON builds a well-formed OrderCreatedEvent, marshals it, and
// returns its OrderID too, so tests can assert ExecuteOrder was called with
// this exact order.
func validEventJSON(t *testing.T) ([]byte, uuid.UUID) {
	t.Helper()
	event := order.OrderCreatedEvent{
		OrderID:       uuid.New(),
		UserID:        uuid.New(),
		AssetSymbol:   "PETR4",
		Quantity:      10,
		Side:          order.SideBuy,
		ExecutionType: order.ExecutionMarket,
		CreatedAt:     time.Now(),
	}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal test event: %v", err)
	}
	return body, event.OrderID
}

func TestHandleOrderCreated(t *testing.T) {
	validBody, validOrderID := validEventJSON(t)

	tests := []struct {
		name            string
		body            []byte
		store           *fakeExecutionStore
		prices          *fakePriceGetter
		wantErr         bool
		wantStoreCalled bool
	}{
		{
			name:            "malformed json",
			body:            []byte(`{json invalido}`),
			store:           &fakeExecutionStore{},
			prices:          &fakePriceGetter{},
			wantErr:         true,
			wantStoreCalled: false,
		},
		{
			name:            "missing required field",
			body:            []byte(`{}`),
			store:           &fakeExecutionStore{},
			prices:          &fakePriceGetter{},
			wantErr:         true,
			wantStoreCalled: false,
		},
		{
			name:            "price lookup fails",
			body:            validBody,
			store:           &fakeExecutionStore{},
			prices:          &fakePriceGetter{err: errors.New("price service unavailable")},
			wantErr:         true,
			wantStoreCalled: false,
		},
		{
			name:            "execution succeeds",
			body:            validBody,
			store:           &fakeExecutionStore{executed: true},
			prices:          &fakePriceGetter{price: 38.42},
			wantErr:         false,
			wantStoreCalled: true,
		},
		{
			name:            "duplicate delivery",
			body:            validBody,
			store:           &fakeExecutionStore{executed: false},
			prices:          &fakePriceGetter{price: 38.42},
			wantErr:         false,
			wantStoreCalled: true,
		},
		{
			name:            "store fails",
			body:            validBody,
			store:           &fakeExecutionStore{err: errors.New("database unavailable")},
			prices:          &fakePriceGetter{price: 38.42},
			wantErr:         true,
			wantStoreCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handleOrderCreated(context.Background(), tt.store, tt.prices, tt.body)

			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Errorf("handleOrderCreated() error = %v, wantErr %t", err, tt.wantErr)
			}

			if tt.store.called != tt.wantStoreCalled {
				t.Errorf("ExecuteOrder called = %t, want %t", tt.store.called, tt.wantStoreCalled)
			}

			if tt.wantStoreCalled && tt.store.called {
				if tt.store.gotID != validOrderID {
					t.Errorf("ExecuteOrder called with order ID %v, want %v", tt.store.gotID, validOrderID)
				}
				if tt.store.gotPrice != tt.prices.price {
					t.Errorf("ExecuteOrder called with price %v, want %v", tt.store.gotPrice, tt.prices.price)
				}
				if tt.store.gotQty != 10 {
					t.Errorf("ExecuteOrder called with quantity %d, want %d", tt.store.gotQty, 10)
				}
			}
		})
	}
}
