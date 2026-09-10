package order

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GabrielHKGodinho/investment-engine/internal/auth"
	"github.com/GabrielHKGodinho/investment-engine/internal/pagination"
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

type Handler struct {
	store OrderStore
}

func NewHandler(store OrderStore) *Handler {
	return &Handler{store: store}
}

func (h *Handler) ListOrders(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserIDFromContext(r.Context())
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	query := r.URL.Query()
	filter := ListFilter{UserID: userID, Limit: defaultLimit}

	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		if n > maxLimit {
			n = maxLimit
		}
		filter.Limit = n
	}

	if raw := query.Get("cursor"); raw != "" {
		c, err := pagination.DecodeCursor(raw)
		if err != nil {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		filter.Cursor = &c
	}

	if raw := query.Get("status"); raw != "" {
		s := Status(strings.ToUpper(raw))
		if !isValidStatus(s) {
			http.Error(w, "invalid status", http.StatusBadRequest)
			return
		}
		filter.Status = &s
	}

	if raw := query.Get("side"); raw != "" {
		s := Side(strings.ToUpper(raw))
		if !isValidSide(s) {
			http.Error(w, "invalid side", http.StatusBadRequest)
			return
		}
		filter.Side = &s
	}

	if raw := query.Get("executionType"); raw != "" {
		e := ExecutionType(strings.ToUpper(raw))
		if !isValidExecutionType(e) {
			http.Error(w, "invalid executionType", http.StatusBadRequest)
			return
		}
		filter.ExecutionType = &e
	}

	if raw := query.Get("assetSymbol"); raw != "" {
		filter.AssetSymbol = &raw
	}

	if raw := query.Get("createdAfter"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			http.Error(w, "invalid createdAfter", http.StatusBadRequest)
			return
		}
		filter.CreatedAfter = &t
	}

	if raw := query.Get("createdBefore"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			http.Error(w, "invalid createdBefore", http.StatusBadRequest)
			return
		}
		filter.CreatedBefore = &t
	}

	orders, nextCursor, err := h.store.List(r.Context(), filter)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
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
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		resp.NextCursor = &token
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
