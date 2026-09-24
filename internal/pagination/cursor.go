package pagination

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Cursor represents the position right after the last item of the previous page.
type Cursor struct {
	CreatedAt time.Time `json:"createdAt"`
	ID        uuid.UUID `json:"id"`
}

// EncodeCursor turns a Cursor into the opaque token sent back to the client.
func EncodeCursor(c Cursor) (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.URLEncoding.EncodeToString(data), nil
}

// DecodeCursor parses a client-supplied token back into a Cursor.
// A decode failure means the client sent a malformed/tampered token —
// callers should surface this as 400 Bad Request, never 500.
func DecodeCursor(token string) (Cursor, error) {
	data, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, fmt.Errorf("decode cursor: invalid encoding: %w", err)
	}

	var c Cursor
	if err := json.Unmarshal(data, &c); err != nil {
		return Cursor{}, fmt.Errorf("decode cursor: invalid payload: %w", err)
	}
	// NEW: a well-formed JSON can still carry no position at all ({} or null).
	if c.CreatedAt.IsZero() || c.ID == uuid.Nil {
		return Cursor{}, errors.New("decode cursor: invalid payload: createdAt and id are required")
	}
	return c, nil
}
