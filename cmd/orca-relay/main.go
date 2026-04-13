package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/weill-labs/orca/internal/relay"
)

// BuildCommit is set at build time.
var BuildCommit string

func main() {
	cfg, err := relay.LoadConfigFromEnv()
	if err != nil {
		log.Printf("load relay config: %v", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:    cfg.Addr(),
		Handler: relay.NewServer(cfg, relay.ServerOptions{}).Handler(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("shutdown relay server: %v", err)
		}
	}()

	commit := BuildCommit
	if commit == "" {
		commit = "dev"
	}
	log.Printf("starting orca-relay %s on %s", commit, cfg.Addr())

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("serve relay server: %v", err)
		os.Exit(1)
	}
}
