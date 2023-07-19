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
	"go.atoms.co/splitter/pkg/util"
	"fmt"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"time"
)

var (
	endpoint, instance, configPath, dataPath *string
	port, internalPort                       *int
)

func makeStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start Splitter server",
		Run:   startFn,
		Args:  cobra.NoArgs,
	}

	endpoint = cmd.PersistentFlags().String("endpoint", "splitter.splitter:50051", "splitter endpoint")
	instance = cmd.PersistentFlags().String("instance", util.GetInstance(), "instance IP to publish")
	configPath = cmd.PersistentFlags().String("config_path", "/app_config/splitter.yaml", "Base config file")
	dataPath = cmd.PersistentFlags().String("data_path", "/data", "Data path")
	port = cmd.PersistentFlags().Int("port", 50051, "grpc server port")
	internalPort = cmd.PersistentFlags().Int("internal_port", 50052, "grpc server port for pod-to-pod traffic")

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

	s := server.New(ctx, cl, loc, *instance, *endpoint)

	// (3) Try to read data path

	entries, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf(ctx, "Failed to read data path, %v", err)
	}

	for _, e := range entries {
		log.Infof(ctx, "Name, %v", e.Name())
	}

	// (2) Start server and await termination

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
		if err := s.Serve(wctx, listener); err != nil {
			log.Errorf(ctx, "Server exited: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer quit.Close()

		listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", *internalPort))
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

	log.Infof(ctx, "Shutting down. Exiting in 20s.")

	cl.AfterFunc(20*time.Second, func() {
		log.Exitf(ctx, "Exited forcefully")
	})

	s.Shutdown(ctx, 20*time.Second)
	wg.Wait()

	log.Infof(ctx, "Exited")
}
