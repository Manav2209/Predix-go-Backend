package repository

import (
	"context"

	"github.com/google/uuid"
)

// UserPositions reads the durable projection for one authenticated account.
// Kept outside sqlc-generated files as a manually maintained repository query.
func (q *Queries) UserPositions(ctx context.Context, userID uuid.UUID) ([]Position, error) {
	rows, err := q.db.Query(ctx, `SELECT user_id,event_id,outcome,shares,avg_price,updated_at FROM positions WHERE user_id=$1 ORDER BY event_id,outcome`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	positions := make([]Position, 0)
	for rows.Next() {
		var position Position
		if err := rows.Scan(&position.UserID, &position.EventID, &position.Outcome, &position.Shares, &position.AvgPrice, &position.UpdatedAt); err != nil {
			return nil, err
		}
		positions = append(positions, position)
	}
	return positions, rows.Err()
}
