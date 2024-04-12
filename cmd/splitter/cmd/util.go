package cmd

import (
	"context"
	"go.atoms.co/lib/log"
	"fmt"
	"net/http"
	"os"
)

func startHealthCheck(ctx context.Context, port int) {
	// Set up http health check
	http.HandleFunc("/health", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	log.Infof(ctx, "Serving health check traffic on port %d", port)
	log.Fatalf(ctx, "health listen and serve failed: %v", http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func getInstance() string {
	if ip := os.Getenv("POD_IP"); len(ip) > 0 {
		return ip
	}
	return "localhost"
}

func getName() string {
	if name := os.Getenv("POD_NAME"); len(name) > 0 {
		return name
	}
	return "local"
}
