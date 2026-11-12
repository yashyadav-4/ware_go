package config

import (
	"os"
	"strings"
)

type Config struct {
	Port         string
	DatabaseURL  string
	RedisAddr    string
	KafkaBrokers string
	GeminiAPIKey string
	GeminiModel  string
}

func loadEnvFile(filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			if os.Getenv(k) == "" {
				_ = os.Setenv(k, v)
			}
		}
	}
}

func Load() *Config {
	loadEnvFile(".env")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgrespassword@localhost:5432/ware_go?sslmode=disable"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:19092"
	}

	geminiKey := os.Getenv("GEMINI_API_KEY")

	geminiModel := os.Getenv("GEMINI_MODEL")
	if geminiModel == "" {
		geminiModel = "gemini-3.5-flash-lite"
	}

	return &Config{
		Port:         port,
		DatabaseURL:  dbURL,
		RedisAddr:    redisAddr,
		KafkaBrokers: kafkaBrokers,
		GeminiAPIKey: geminiKey,
		GeminiModel:  geminiModel,
	}
}