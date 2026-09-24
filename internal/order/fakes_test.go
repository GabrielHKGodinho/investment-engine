package order

import (
	"context"

	"github.com/GabrielHKGodinho/investment-engine/internal/pagination"
)

// fakeOrderStore is a hand-written fake of OrderStore. Each case in a table
// gets its own instance, so tests stay isolated and can run in parallel.
type fakeOrderStore struct {
	// What the fake returns.
	listOrders []Order
	listNext   *pagination.Cursor
	listErr    error
	createErr  error

	// What the fake recorded about how it was called.
	listCalls   int
	gotFilter   ListFilter
	createCalls int
	gotCreated  Order
}

func (f *fakeOrderStore) List(ctx context.Context, filter ListFilter) ([]Order, *pagination.Cursor, error) {
	f.listCalls++
	f.gotFilter = filter
	return f.listOrders, f.listNext, f.listErr
}

func (f *fakeOrderStore) Create(ctx context.Context, o Order) (Order, error) {
	f.createCalls++
	f.gotCreated = o
	if f.createErr != nil {
		return Order{}, f.createErr
	}
	return o, nil
}

// fakePublisher is a hand-written fake of EventPublisher.
type fakePublisher struct {
	// What the fake returns.
	err error

	// What the fake recorded about how it was called.
	calls int
	got   OrderCreatedEvent
}

func (f *fakePublisher) PublishOrderCreated(ctx context.Context, event OrderCreatedEvent) error {
	f.calls++
	f.got = event
	return f.err
}
