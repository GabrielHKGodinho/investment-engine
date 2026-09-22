package priceservice

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pricepb "github.com/GabrielHKGodinho/investment-engine/internal/priceservice/pb"
)

// Client wraps the generated PriceService gRPC client, hiding the connection
// and request/response types from the rest of the codebase.
type Client struct {
	conn   *grpc.ClientConn
	client pricepb.PriceServiceClient
}

// NewClient dials the PriceService at target (e.g. "localhost:50051") and
// returns a ready-to-use Client. grpc.NewClient does not block until the
// connection is actually needed — it connects lazily on the first RPC.
func NewClient(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating new client: %w", err)
	}

	client := pricepb.NewPriceServiceClient(conn)

	return &Client{conn: conn, client: client}, nil
}

// GetPrice returns the current simulated price for symbol.
func (c *Client) GetPrice(ctx context.Context, symbol string) (float64, error) {
	req := &pricepb.GetQuoteRequest{Symbol: symbol}

	resp, err := c.client.GetQuote(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("getting price: %w", err)
	}

	return resp.Price, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
