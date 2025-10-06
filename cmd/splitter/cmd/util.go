package cmd

import (
	"context"
	"fmt"
	"net/http"

	"go.atoms.co/lib/log"
)

func startHealthCheck(ctx context.Context, port int) {
	// Set up http health check
	http.HandleFunc("/health", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	log.Infof(ctx, "Serving health check traffic on port %d", port)
	log.Fatalf(ctx, "health listen and serve failed: %v", http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}
