package config

import (
	"os"
)

type Config struct {
	Port string
	DatabaseURL string
	RedisAddr string
}

func Load() *Config{
	port := os.Getenv("PORT")
	if port == ""{
		port = "8080"
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == ""{
		dbURL = "postgres://postgres:postgrespassword@localhost:5432/ware_go?sslmode=disable"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	return &Config{
		Port: port,
		DatabaseURL: dbURL,
		RedisAddr: redisAddr,
	}
}