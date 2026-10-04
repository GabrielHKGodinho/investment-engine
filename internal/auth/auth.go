package auth

import (
	"context"

	"github.com/google/uuid"
)

// TODO: replace with real authentication (see Known limitations in the README).
// Until then, every request is treated as coming from this fixed user.
var simulatedUserID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

func UserIDFromContext(ctx context.Context) (uuid.UUID, error) {
	return simulatedUserID, nil
}
