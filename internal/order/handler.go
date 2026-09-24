package order

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GabrielHKGodinho/investment-engine/internal/apierror"
	"github.com/GabrielHKGodinho/investment-engine/internal/auth"
	"github.com/GabrielHKGodinho/investment-engine/internal/pagination"
	"github.com/google/uuid"
)

const (
	defaultLimit = 20
	maxLimit     = 100
)

type OrderResponse struct {
	OrderID       string    `json:"orderID"`
	AssetSymbol   string    `json:"assetSymbol"`
	Quantity      int       `json:"quantity"`
	Side          string    `json:"side"`
	ExecutionType string    `json:"executionType"`
	LimitPrice    *float64  `json:"limitPrice,omitempty"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"createdAt"`
}

type ListOrdersResponse struct {
	Orders     []OrderResponse `json:"orders"`
	NextCursor *string         `json:"nextCursor"`
	HasMore    bool            `json:"hasMore"`
}

// EventPublisher is what the Handler needs to announce that an order was
// created. It is declared here, next to its only user, so the handler does
// not depend on RabbitMQ: *Publisher satisfies it implicitly.
type EventPublisher interface {
	PublishOrderCreated(ctx context.Context, event OrderCreatedEvent) error
}

type Handler struct {
	store     OrderStore
	publisher EventPublisher
}

func NewHandler(store OrderStore, publisher EventPublisher) *Handler {
	return &Handler{store: store, publisher: publisher}
}

func (h *Handler) ListOrders(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserIDFromContext(r.Context())
	if err != nil {
		apierror.Write(w, http.StatusUnauthorized, apierror.CodeUnauthorized, "authentication required", nil)
		return
	}

	query := r.URL.Query()
	filter := ListFilter{UserID: userID, Limit: defaultLimit}
	var fieldErrors []apierror.FieldError

	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			fieldErrors = append(fieldErrors, apierror.FieldError{Field: "limit", Message: "must be a positive integer"})
		} else {
			if n > maxLimit {
				n = maxLimit
			}
			filter.Limit = n
		}
	}

	if raw := query.Get("cursor"); raw != "" {
		c, err := pagination.DecodeCursor(raw)
		if err != nil {
			fieldErrors = append(fieldErrors, apierror.FieldError{Field: "cursor", Message: "malformed cursor token"})
		} else {
			filter.Cursor = &c
		}
	}

	if raw := query.Get("status"); raw != "" {
		s := Status(strings.ToUpper(raw))
		if !isValidStatus(s) {
			fieldErrors = append(fieldErrors, apierror.FieldError{Field: "status", Message: "must be one of PENDING, EXECUTED, CANCELLED, REJECTED"})
		} else {
			filter.Status = &s
		}
	}

	if raw := query.Get("side"); raw != "" {
		s := Side(strings.ToUpper(raw))
		if !isValidSide(s) {
			fieldErrors = append(fieldErrors, apierror.FieldError{Field: "side", Message: "must be BUY or SELL"})
		} else {
			filter.Side = &s
		}
	}

	if raw := query.Get("executionType"); raw != "" {
		e := ExecutionType(strings.ToUpper(raw))
		if !isValidExecutionType(e) {
			fieldErrors = append(fieldErrors, apierror.FieldError{Field: "executionType", Message: "must be MARKET or LIMIT"})
		} else {
			filter.ExecutionType = &e
		}
	}

	if raw := query.Get("assetSymbol"); raw != "" {
		filter.AssetSymbol = &raw
	}

	if raw := query.Get("createdAfter"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			fieldErrors = append(fieldErrors, apierror.FieldError{Field: "createdAfter", Message: "must be an RFC3339 timestamp"})
		} else {
			filter.CreatedAfter = &t
		}
	}

	if raw := query.Get("createdBefore"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			fieldErrors = append(fieldErrors, apierror.FieldError{Field: "createdBefore", Message: "must be an RFC3339 timestamp"})
		} else {
			filter.CreatedBefore = &t
		}
	}

	if len(fieldErrors) > 0 {
		apierror.Write(w, http.StatusBadRequest, apierror.CodeValidation, "one or more query parameters are invalid", fieldErrors)
		return
	}

	orders, nextCursor, err := h.store.List(r.Context(), filter)
	if err != nil {
		apierror.Write(w, http.StatusInternalServerError, apierror.CodeInternal, "internal error", nil)
		return
	}

	resp := ListOrdersResponse{
		Orders:  make([]OrderResponse, len(orders)),
		HasMore: nextCursor != nil,
	}
	for i, o := range orders {
		resp.Orders[i] = OrderResponse{
			OrderID:       o.ID.String(),
			AssetSymbol:   o.AssetSymbol,
			Quantity:      o.Quantity,
			Side:          string(o.Side),
			ExecutionType: string(o.ExecutionType),
			LimitPrice:    o.LimitPrice,
			Status:        string(o.Status),
			CreatedAt:     o.CreatedAt,
		}
	}
	if nextCursor != nil {
		token, err := pagination.EncodeCursor(*nextCursor)
		if err != nil {
			apierror.Write(w, http.StatusInternalServerError, apierror.CodeInternal, "internal error", nil)
			return
		}
		resp.NextCursor = &token
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

type CreateOrderRequest struct {
	AssetSymbol   string   `json:"assetSymbol"`
	Quantity      int      `json:"quantity"`
	Side          string   `json:"side"`
	ExecutionType string   `json:"executionType"`
	LimitPrice    *float64 `json:"limitPrice,omitempty"`
}

func (h *Handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserIDFromContext(r.Context())
	if err != nil {
		apierror.Write(w, http.StatusUnauthorized, apierror.CodeUnauthorized, "authentication required", nil)
		return
	}

	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.Write(w, http.StatusBadRequest, apierror.CodeValidation, "malformed request body", nil)
		return
	}

	var fieldErrors []apierror.FieldError

	assetSymbol := strings.ToUpper(strings.TrimSpace(req.AssetSymbol))
	if assetSymbol == "" {
		fieldErrors = append(fieldErrors, apierror.FieldError{Field: "assetSymbol", Message: "is required"})
	}
	if req.Quantity <= 0 {
		fieldErrors = append(fieldErrors, apierror.FieldError{Field: "quantity", Message: "must be a positive integer"})
	}

	side := Side(strings.ToUpper(req.Side))
	if !isValidSide(side) {
		fieldErrors = append(fieldErrors, apierror.FieldError{Field: "side", Message: "must be BUY or SELL"})
	}

	executionType := ExecutionType(strings.ToUpper(req.ExecutionType))
	if !isValidExecutionType(executionType) {
		fieldErrors = append(fieldErrors, apierror.FieldError{Field: "executionType", Message: "must be MARKET or LIMIT"})
	} else if executionType == ExecutionLimit && (req.LimitPrice == nil || *req.LimitPrice <= 0) {
		fieldErrors = append(fieldErrors, apierror.FieldError{Field: "limitPrice", Message: "is required and must be positive for LIMIT orders"})
	} else if executionType == ExecutionMarket && req.LimitPrice != nil {
		fieldErrors = append(fieldErrors, apierror.FieldError{Field: "limitPrice", Message: "must not be set for MARKET orders"})
	}

	if len(fieldErrors) > 0 {
		apierror.Write(w, http.StatusBadRequest, apierror.CodeValidation, "one or more fields are invalid", fieldErrors)
		return
	}

	order := Order{
		ID:            uuid.New(),
		UserID:        userID,
		AssetSymbol:   assetSymbol,
		Quantity:      req.Quantity,
		Side:          side,
		ExecutionType: executionType,
		LimitPrice:    req.LimitPrice,
		Status:        StatusPending,
	}

	created, err := h.store.Create(r.Context(), order)
	if err != nil {
		apierror.Write(w, http.StatusInternalServerError, apierror.CodeInternal, "internal error", nil)
		return
	}

	event := NewOrderCreatedEvent(created)
	if err := h.publisher.PublishOrderCreated(r.Context(), event); err != nil {
		log.Printf("failed to publish order created event for order %s: %v", created.ID, err)
		// The order was persisted successfully — that's what "created" means to
		// the client. The event failing to publish is a real problem, but not
		// one that should make this request look like it failed entirely.
	}

	resp := OrderResponse{
		OrderID:       created.ID.String(),
		AssetSymbol:   created.AssetSymbol,
		Quantity:      created.Quantity,
		Side:          string(created.Side),
		ExecutionType: string(created.ExecutionType),
		LimitPrice:    created.LimitPrice,
		Status:        string(created.Status),
		CreatedAt:     created.CreatedAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}
