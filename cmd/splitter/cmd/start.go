package cmd

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/log/hclog"
	"go.atoms.co/lib/service/envoyx"
	"go.atoms.co/lib/service/locationx"
	"go.atoms.co/lib/service/metricsx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/signalx"
	"go.atoms.co/lib/yamlx"
	"go.atoms.co/splitter/pkg/server"
	"go.atoms.co/splitter/pkg/storage/raftstorage"
	"fmt"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/spf13/cobra"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"
)

type conf struct{}

func makeStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start Splitter server",
		Args:  cobra.NoArgs,
	}

	instance := cmd.PersistentFlags().String("instance", getInstance(), "instance IP to publish")
	configPath := cmd.PersistentFlags().String("config_path", "/app_config/splitter.yaml", "Base config file")
	dataPath := cmd.PersistentFlags().String("data_path", "/data", "Data path")
	port := cmd.PersistentFlags().Int("port", 50051, "grpc server port")
	internalPort := cmd.PersistentFlags().Int("internal_port", 50052, "grpc server port for pod-to-pod traffic")
	healthPort := cmd.PersistentFlags().Int("health_port", 8081, "http port for health check traffic")
	pprofPort := cmd.PersistentFlags().Int("pprof_port", 6060, "http port for pprof debug traffic")

	raftPort := cmd.PersistentFlags().Int("raft_port", 50053, "tcp port for raft traffic")
	raftID := cmd.PersistentFlags().String("raft_id", "id1", "Node id used by Raft")
	bootstrap := cmd.PersistentFlags().Bool("bootstrap", false, "Bootstrap raft node")

	cmd.Run = func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		cl := clock.New()

		// (1) Initialize

		envoyx.EnsureReady(ctx, envoyx.WaitTimeout)
		metricsx.Init(ctx, "splitter")
		go startPprofHandler(ctx, *pprofPort)

		loc := locationx.New()

		cfg, err := yamlx.ReadFile[conf](*configPath)
		if err != nil {
			log.Exitf(ctx, "Failed to load configuration: %v", err)
		}

		_ = cfg
		_ = instance

		baseDir := filepath.Join(*dataPath, *raftID)
		if _, err := os.Stat(baseDir); os.IsNotExist(err) {
			if err := os.Mkdir(baseDir, 0700); err != nil {
				log.Fatalf(ctx, "Failed to make base raft path", err)
			}
		}

		fsm, err := raftstorage.NewFSM(baseDir)
		if err != nil {
			log.Fatalf(ctx, "failed to create FSM", err)
		}

		ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
		if err != nil {
			log.Fatalf(ctx, "failed to create boltdb log store", err)
		}

		sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
		if err != nil {
			log.Fatalf(ctx, "failed to create boltdb stable store", err)
		}

		fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
		if err != nil {
			log.Fatalf(ctx, "failed to create file snapshot store", err)
		}

		bindAddr := fmt.Sprintf("localhost:%v", *raftPort)

		tcpAddr, err := net.ResolveTCPAddr("tcp", bindAddr)
		if err != nil {
			log.Fatalf(ctx, "failed to resolve TCP addr", err)
		}

		hclogger := hclog.New(ctx, "", log.SevDebug)

		// https://github.com/yusufsyaifudin/raft-sample/blob/master/cmd/api/main.go#L52
		trans, err := raft.NewTCPTransportWithLogger(bindAddr, tcpAddr, 3, 10*time.Second, hclogger)
		if err != nil {
			log.Fatalf(ctx, "failed to setup raft tcp transport", err)
		}

		raftConf := raft.DefaultConfig()
		raftConf.LocalID = raft.ServerID(*raftID)
		raftConf.Logger = hclogger

		raftObj, err := raft.NewRaft(raftConf, fsm, ldb, sdb, fss, trans)
		if err != nil {
			log.Fatalf(ctx, "Failed to initialize raft instance", err)
		}

		leaderCh := make(chan raft.Observation, 16)
		observer := raft.NewObserver(leaderCh, true, func(o *raft.Observation) bool {
			_, ok := o.Data.(raft.LeaderObservation)
			return ok
		})
		raftObj.RegisterObserver(observer)

		quitObs := iox.NewAsyncCloser()
		go func() {
			for {
				select {
				case obs := <-leaderCh:
					leaderObs, ok := obs.Data.(raft.LeaderObservation)
					if !ok {
						log.Debugf(ctx, "Got unknown observation type from raft", "type", reflect.TypeOf(obs.Data))
						continue
					}

					//s.grpcLeaderForwarder.UpdateLeaderAddr(s.config.Datacenter, string(leaderObs.LeaderAddr))
					//s.peeringBackend.SetLeaderAddress(string(leaderObs.LeaderAddr))
					//s.raftStorageBackend.LeaderChanged()
					//s.controllerManager.SetRaftLeader(s.IsLeader())

					log.Infof(ctx, "Got observation", leaderObs)

				case <-quitObs.Closed():
					raftObj.DeregisterObserver(observer)
					return
				}
			}
		}()

		if *bootstrap {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(*raftID),
						Address: trans.LocalAddr(),
					},
				},
			}
			f := raftObj.BootstrapCluster(configuration)
			if err := f.Error(); err != nil {
				log.Fatalf(ctx, "Failed to bootstrap raft cluster", err)
			}
		}

		_, mgmt := raftstorage.New(cl, fsm, raftObj)

		s := server.New(ctx, cl, loc, leaderCh, raftObj, mgmt)

		// (2) Start server and await termination

		quit := iox.NewAsyncCloser()
		wctx, cancel := contextx.WithQuitCancel(ctx, quit.Closed())

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer quit.Close()

			listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%v", *port))
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

			listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%v", *internalPort))
			if err != nil {
				log.Errorf(ctx, "failed to open port %v: %v", *internalPort, err)
				return
			}
			log.Infof(ctx, "Serving internal traffic on port %d", *internalPort)
			if err := s.ServeInternal(wctx, listener); err != nil {
				log.Errorf(ctx, "Server exited: %v", err)
			}
		}()

		go func() {
			defer quit.Close()

			sig := <-signalx.InterruptChan()
			log.Infof(ctx, "Received '%v' signal. Exiting", sig)
		}()

		// start health check after server components initialized
		go startHealthCheck(wctx, *healthPort)

		<-quit.Closed()
		cancel()
		quitObs.Close()

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
