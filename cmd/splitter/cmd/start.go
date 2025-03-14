package cmd

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/spf13/cobra"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/encoding/yamlx"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/log/hclog"
	"go.atoms.co/lib/service/envoyx"
	"go.atoms.co/lib/service/locationx"
	"go.atoms.co/lib/service/metricsx"
	"go.atoms.co/lib/service/pprofx"
	"go.atoms.co/lib/tracing"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/signalx"
	"go.atoms.co/splitter/pkg/cluster"
	"go.atoms.co/splitter/pkg/server"
	"go.atoms.co/splitter/pkg/service/leader"
	raftstorage "go.atoms.co/splitter/pkg/storage/raft"
)

type conf struct{}

func makeStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start Splitter server",
		Args:  cobra.NoArgs,
	}

	configPath := cmd.PersistentFlags().String("config_path", "/app_config/splitter.yaml", "Base config file")
	dataPath := cmd.PersistentFlags().String("data_path", "/data", "Data path")
	port := cmd.PersistentFlags().Int("port", 50051, "Grpc server port")
	healthPort := cmd.PersistentFlags().Int("health_port", 8081, "Http port for health check traffic")
	fastActivation := cmd.PersistentFlags().Bool("fast_activation", false, "Fast Leader/Coordinator activation for testing")

	splitterServer := cmd.PersistentFlags().String("splitter_server", "", "Server address used by Splitter")
	splitterPort := cmd.PersistentFlags().Int("splitter_port", 50052, "Grpc server port for pod-to-pod traffic")

	raftID := cmd.PersistentFlags().String("raft_id", getName(), "Node id used by Raft")
	raftServer := cmd.PersistentFlags().String("raft_server", "", "Server address used by Raft")
	raftPort := cmd.PersistentFlags().Int("raft_port", 50053, "Tcp port for raft traffic")
	raftFastBootstrap := cmd.PersistentFlags().Bool("raft_fast_bootstrap", false, "Fast Raft bootstrap for testing")
	raftJoinPeers := cmd.PersistentFlags().StringSlice("raft_join_peers", []string{}, "Raft peers to join including self")

	cmd.Run = func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		cl := clock.New()

		// (1) Initialize

		envoyx.EnsureReady(ctx, envoyx.WaitTimeout)
		metricsx.Init(ctx, "splitter")
		err := tracing.RegisterExporter("")
		if err != nil {
			log.Warnf(ctx, "Failed to register tracer: %v", err)
		}

		go pprofx.Start(ctx)

		loc := locationx.New()

		cfg, err := yamlx.UnmarshalFromFile[conf](*configPath)
		if err != nil {
			log.Exitf(ctx, "Failed to load configuration: %v", err)
		}

		_ = cfg

		baseDir := filepath.Join(*dataPath, *raftID)
		if _, err := os.Stat(baseDir); os.IsNotExist(err) {
			if err := os.Mkdir(baseDir, 0700); err != nil {
				log.Fatalf(ctx, "Failed to make base raft path: %v", err)
			}
		}

		// (2) Set up raft node

		ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
		if err != nil {
			log.Fatalf(ctx, "failed to create boltdb log store: %v", err)
		}

		sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
		if err != nil {
			log.Fatalf(ctx, "failed to create boltdb stable store: %v", err)
		}

		fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
		if err != nil {
			log.Fatalf(ctx, "failed to create file snapshot store: %v", err)
		}

		bindAddr := fmt.Sprintf("0.0.0.0:%v", *raftPort)

		tcpAddr, err := net.ResolveTCPAddr("tcp", *raftServer)
		if err != nil {
			log.Fatalf(ctx, "failed to resolve TCP addr: %v", err)
		}

		hclogger := hclog.New(ctx, "", log.SevDebug)

		// MsgpackUseNewTimeFormat=true https://github.com/hashicorp/raft/releases/tag/v1.6.0. Needed since we run go-msgpack v1+
		config := &raft.NetworkTransportConfig{MaxPool: 9, Timeout: 10 * time.Second, Logger: hclogger, MsgpackUseNewTimeFormat: true}
		trans, err := raft.NewTCPTransportWithConfig(bindAddr, tcpAddr, config)
		if err != nil {
			log.Fatalf(ctx, "failed to setup raft tcp transport: %v", err)
		}

		raftConf := raft.DefaultConfig()
		raftConf.LocalID = raft.ServerID(*raftID)
		raftConf.Logger = hclogger

		fsm := raftstorage.NewFSM()

		r, err := raft.NewRaft(raftConf, fsm, ldb, sdb, fss, trans)
		if err != nil {
			log.Fatalf(ctx, "Failed to initialize raft instance: %v", err)
		}

		// (3) Initialize Server components and Server

		storage := raftstorage.New(cl, raft.ServerID(*raftID), r, fsm)

		var opts []cluster.Option
		if *raftFastBootstrap {
			opts = append(opts, cluster.WithFastBootstrap())
		}
		c, directives := cluster.New(cl, raft.ServerID(*raftID), raft.ServerAddress(*raftServer), r, ldb, sdb, trans, *raftJoinPeers, *splitterPort, opts...)

		var lopts []leader.Option
		if *fastActivation {
			lopts = append(lopts, leader.WithFastActivation())
		}
		manager := leader.NewManager(cl, directives, func(ctx context.Context) (iox.AsyncCloser, leader.Proxy) {
			ret := leader.New(ctx, cl, loc, storage, lopts...)
			return ret, ret
		})

		s := server.New(ctx, cl, loc, *splitterServer, c, manager, server.WithFastActivation(*fastActivation))

		// (4) Start server and await termination

		quit := iox.NewAsyncCloser()
		wctx, cancel := contextx.WithQuitCancel(ctx, quit.Closed())

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer quit.Close()

			listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", *port))
			if err != nil {
				log.Errorf(ctx, "failed to open port %v: %v", *port, err)
				return
			}
			log.Infof(ctx, "Serving public traffic on port %d", *port)
			if err := s.Serve(wctx, listener); err != nil {
				log.Errorf(ctx, "Server exited: %v", err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer quit.Close()

			listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", *splitterPort))
			if err != nil {
				log.Errorf(ctx, "failed to open port %v: %v", *splitterPort, err)
				return
			}
			log.Infof(ctx, "Serving internal traffic on port %d", *splitterPort)
			if err := s.ServeInternal(wctx, listener); err != nil {
				log.Errorf(ctx, "Server exited: %v", err)
			}
		}()

		go func() {
			defer quit.Close()

			sig := <-signalx.InterruptChan()
			log.Infof(ctx, "Received '%v' signal. Exiting", sig)
		}()

		// (5) Start health check after server components initialized

		go startHealthCheck(wctx, *healthPort)

		<-quit.Closed()
		cancel()

		log.Infof(ctx, "Shutting down. Exiting in 20s.")

		cl.AfterFunc(20*time.Second, func() {
			log.Exitf(ctx, "Exited forcefully")
		})

		s.Shutdown(ctx, 20*time.Second)
		wg.Wait()

		log.Infof(ctx, "Exited")
	}

	return cmd
}
