package util

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"go.atoms.co/lib/log"
)

var pprofPort = 6060
var healthPort = 8081

func NewInterruptChan() <-chan os.Signal {
	var stopChan = make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	return stopChan
}

func StartPprofHandler(ctx context.Context) {
	// start the default pprof handlers on the pprof port: https://golang.org/pkg/net/http/pprof/.
	log.Infof(ctx, "Setting up pprof on port: %v", pprofPort)
	log.Fatal(ctx, http.ListenAndServe(fmt.Sprintf(":%v", pprofPort), nil))
}

func StartHealthCheck(ctx context.Context) {
	// Set up http health check
	http.HandleFunc("/health", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	log.Infof(ctx, "Serving health check traffic on port %d", healthPort)
	log.Fatalf(ctx, "health listen and serve failed: %v", http.ListenAndServe(fmt.Sprintf(":%d", healthPort), nil))
}

func GetInstance() string {
	if ip := os.Getenv("POD_IP"); len(ip) > 0 {
		return ip
	}
	return "localhost"
}
