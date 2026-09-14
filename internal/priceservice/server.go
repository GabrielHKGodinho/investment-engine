package priceservice

import (
	"context"
	"math/rand"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pricepb "github.com/GabrielHKGodinho/investment-engine/internal/priceservice/pb"
)

// Server implements the PriceService gRPC contract using simulated prices,
// consistent with the project's decision to fake execution against a
// simulated market price instead of a real order book.
type Server struct {
	pricepb.UnimplementedPriceServiceServer

	basePrices map[string]float64
}

// NewServer builds a Server with a fixed set of simulated base prices.
func NewServer() *Server {
	return &Server{
		basePrices: map[string]float64{
			"PETR4": 38.42,
			"VALE3": 61.15,
			"ITUB4": 32.90,
		},
	}
}

// GetQuote returns a simulated quote for the requested symbol, applying a
// small random variation around the base price to mimic a live market feed.
func (s *Server) GetQuote(ctx context.Context, req *pricepb.GetQuoteRequest) (*pricepb.GetQuoteResponse, error) {
	base, ok := s.basePrices[req.Symbol]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown symbol: %s", req.Symbol)
	}

	variation := (rand.Float64() - 0.5) * 0.02 // +/- 1% simulated movement
	price := base * (1 + variation)

	return &pricepb.GetQuoteResponse{
		Symbol:        req.Symbol,
		Price:         price,
		TimestampUnix: time.Now().Unix(),
	}, nil
}
