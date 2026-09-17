package dto

import "time"

type CreateEventRequest struct {
	Title       string `json:"title" binding:"required"`
	Description string `json:"description" binding:"required"`
	Question    string `json:"question" binding:"required"`
	ImageURL    string `json:"imageurl" binding:"required,url"`
	ExpiresAt   string `json:"expiresAt" binding:"required"` // ISO timestamp
}

type EventResponse struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Question    string    `json:"question"`
	Thumbnail   string    `json:"thumbnail"`
	IsActive    bool      `json:"isActive"`
	ExpiresAt   time.Time `json:"expiresAt"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}
