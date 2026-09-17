package handler

import (
	"predix/internal/repository"
	"predix/pkg/redis"
)

type Handler struct {
	Queries      *repository.Queries
	RedisManager *redis.RedisManager
}

func NewHandler(q *repository.Queries, rm *redis.RedisManager) *Handler {
	return &Handler{
		Queries:      q,
		RedisManager: rm,
	}
}
