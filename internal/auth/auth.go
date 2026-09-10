package auth

import (
	"context"

	"github.com/google/uuid"
)

// TODO: substituir por autenticação de verdade quando essa fase do roadmap chegar.
// Por enquanto, toda requisição é tratada como vinda deste usuário fixo.
var simulatedUserID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

func UserIDFromContext(ctx context.Context) (uuid.UUID, error) {
	return simulatedUserID, nil
}
