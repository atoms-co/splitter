package cmd

import (
	"context"
	"atoms.co/flagship"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/service/envoyx"
	"go.atoms.co/lib/service/flagshipx"
	"go.atoms.co/lib/service/locationx"
	"go.atoms.co/lib/service/logx"
	"go.atoms.co/lib/service/metricsx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/signalx"
	"go.atoms.co/splitter/pkg/server"
	"go.atoms.co/splitter/pkg/storage/raftstorage"
	"go.atoms.co/splitter/pkg/util"
	"fmt"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"
)

var (
	configPath, dataPath, raftID *string
	port, internalPort, raftPort *int
	bootstrap                    *bool
)

func makeStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start Splitter server",
		Run:   startFn,
		Args:  cobra.NoArgs,
	}

	configPath = cmd.PersistentFlags().String("config_path", "/app_config/splitter.yaml", "Base config file")
	port = cmd.PersistentFlags().Int("port", 50051, "grpc server port")
	internalPort = cmd.PersistentFlags().Int("internal_port", 50052, "grpc server port for pod-to-pod traffic")
	raftPort = cmd.PersistentFlags().Int("raft_port", 50053, "tcp port for raft traffic")
	dataPath = cmd.PersistentFlags().String("data_path", "/tmp", "Data path")
	raftID = cmd.PersistentFlags().String("raft_id", "id1", "Node id used by Raft")
	bootstrap = cmd.PersistentFlags().Bool("bootstrap", false, "Bootstrap raft node")

	return cmd
}

type splitterConfig struct{}

func loadConfig(ctx context.Context, configFilePath string) splitterConfig {
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Exitf(ctx, "Failed to load configuration: %v", err)
	}
	var cfg splitterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Exitf(ctx, "Failed to parse configuration: %v", err)
	}
	return cfg
}

func startFn(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	cl := clock.New()
	logx.Init(context.Background())

	// (1) Initialize

	envoyx.EnsureReady(ctx, envoyx.WaitTimeout)
	flagshipx.Init(ctx, "splitter", 2*flagship.Timeout)

	metricsx.Init(ctx, "splitter")
	go util.StartPprofHandler(ctx)

	loc := locationx.New()

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

	// https://github.com/yusufsyaifudin/raft-sample/blob/master/cmd/api/main.go#L52
	trans, err := raft.NewTCPTransport(bindAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf(ctx, "failed to setup raft tcp transport", err)
	}

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(*raftID)

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
	go util.StartHealthCheck(wctx)

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
