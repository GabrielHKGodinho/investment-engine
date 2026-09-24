package pagination

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	tests := []struct {
		name   string
		cursor Cursor
	}{
		{
			name: "typical cursor",
			cursor: Cursor{
				CreatedAt: time.Date(2026, 9, 21, 12, 0, 0, 123456789, time.UTC),
				ID:        id,
			},
		},
		{
			name: "not UTC time",
			cursor: Cursor{
				CreatedAt: time.Date(2026, 9, 21, 12, 0, 0, 123456789, time.FixedZone("BRT", -3*60*60)),
				ID:        id,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := EncodeCursor(tt.cursor)
			if err != nil {
				t.Fatalf("EncodeCursor() error = %v", err)
			}

			got, err := DecodeCursor(token)
			if err != nil {
				t.Fatalf("DecodeCursor(%q) error = %v", token, err)
			}

			// time.Time must be compared with Equal: == also compares location and
			// monotonic clock reading, which JSON does not preserve.
			if !got.CreatedAt.Equal(tt.cursor.CreatedAt) {
				t.Errorf("CreatedAt = %s, want %s", got.CreatedAt, tt.cursor.CreatedAt)
			}
			if got.ID != tt.cursor.ID {
				t.Errorf("ID = %s, want %s", got.ID, tt.cursor.ID)
			}
		})
	}
}

func TestDecodeCursor_Invalid(t *testing.T) {
	const validID = "11111111-1111-1111-1111-111111111111"
	const validTime = "2026-09-21T12:00:00Z"

	tests := []struct {
		name            string
		token           string
		wantErrContains string
	}{
		{
			name:            "not base64",
			token:           "%%% not base64 %%%",
			wantErrContains: "invalid encoding",
		},
		{
			name:            "valid base64 but not JSON",
			token:           base64.URLEncoding.EncodeToString([]byte("hello")),
			wantErrContains: "invalid payload",
		},
		{
			name:            "wrong JSON type",
			token:           base64.URLEncoding.EncodeToString([]byte(`[]`)),
			wantErrContains: "invalid payload",
		},
		{
			name:            "invalid createdAt time",
			token:           base64.URLEncoding.EncodeToString([]byte(`{"createdAt":"garbage","id":"` + validID + `"}`)),
			wantErrContains: "invalid payload",
		},
		{
			name:            "empty string",
			token:           "",
			wantErrContains: "invalid payload",
		},
		{
			name:            "empty object",
			token:           base64.URLEncoding.EncodeToString([]byte(`{}`)),
			wantErrContains: "invalid payload",
		},
		{
			name:            "JSON null",
			token:           base64.URLEncoding.EncodeToString([]byte(`null`)),
			wantErrContains: "invalid payload",
		},
		{
			name:            "createdAt only",
			token:           base64.URLEncoding.EncodeToString([]byte(`{"createdAt":"` + validTime + `"}`)),
			wantErrContains: "invalid payload",
		},
		{
			name:            "id only",
			token:           base64.URLEncoding.EncodeToString([]byte(`{"id":"` + validID + `"}`)),
			wantErrContains: "invalid payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeCursor(tt.token)
			if err == nil {
				t.Fatalf("DecodeCursor(%q) returned no error, want one containing %q", tt.token, tt.wantErrContains)
			}
			if !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Errorf("DecodeCursor(%q) error = %q, want it to contain %q", tt.token, err, tt.wantErrContains)
			}
		})
	}
}
