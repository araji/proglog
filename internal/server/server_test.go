package server

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	apiv1 "github.com/araji/proglog/api/v1"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"

	"github.com/araji/proglog/internal/auth"
	"github.com/araji/proglog/internal/config"
	"github.com/araji/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "enable observability debugging")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient apiv1.LogClient,
		nobodyClient apiv1.LogClient,
		config *Config,
	){
		"produce and consume a record":  testProduceConsume,
		"produce and stream of records": testProduceConsumeStream,
		"consume out of range error":    testConsumePastBoundary,
		"unauthorized fails":            testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient,
				nobodyClient,
				config,
				teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}
func testUnauthorized(t *testing.T, _, nobodyClient apiv1.LogClient, config *Config) {

	ctx := context.Background()
	produce, err := nobodyClient.Produce(
		ctx, &apiv1.ProduceRequest{
			Record: &apiv1.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatal("expected produce to be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code %d, want %d", gotCode, wantCode)
	}
	consume, err := nobodyClient.Consume(
		ctx, &apiv1.ConsumeRequest{
			Offset: 0,
		},
	)
	if consume != nil {
		t.Fatal("expected consume to be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code %d, want %d", gotCode, wantCode)
	}

}

func testProduceConsume(t *testing.T, client, _ apiv1.LogClient, config *Config) {
	req := &apiv1.ProduceRequest{
		Record: &apiv1.Record{
			Value: []byte("hello world"),
		},
	}
	res, err := client.Produce(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uint64(0), res.Offset)
	record, err := config.CommitLog.Read(res.Offset)
	require.NoError(t, err)
	require.Equal(t, req.Record.Value, record.Value)
}
func testProduceConsumeStream(t *testing.T, client, _ apiv1.LogClient, config *Config) {
	ctx := context.Background()
	records := []*apiv1.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {

			err := stream.Send(&apiv1.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, uint64(offset), res.Offset)

		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &apiv1.ConsumeRequest{Offset: 0})
		require.NoError(t, err)
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &apiv1.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}
func testConsumePastBoundary(t *testing.T, client, _ apiv1.LogClient, config *Config) {

	ctx := context.Background()

	produce, err := client.Produce(ctx, &apiv1.ProduceRequest{
		Record: &apiv1.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &apiv1.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("expected consume to be nil")
	}
	got := grpc.Code(err)
	want := grpc.Code(apiv1.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func setupTest(t *testing.T, fn func(*Config)) (rootClient, nobodyClient apiv1.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	clientTLSConfig, err := config.SetupTlsConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		CertFile: config.RootClientCertFile,
		KeyFile:  config.RootClientKeyFile,
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(clientTLSConfig)
	cc, err := grpc.Dial(
		l.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
	)
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		apiv1.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTlsConfig(config.TLSConfig{
			CAFile:   config.CAFile,
			CertFile: crtPath,
			KeyFile:  keyPath,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := apiv1.NewLogClient(conn)
		return conn, client, opts
	}
	//var rootConn *grpc.ClientConn
	rootConn, rootClient, _ := newClient(config.RootClientCertFile, config.RootClientKeyFile)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	serverTLSConfig, err := config.SetupTlsConfig(config.TLSConfig{
		CAFile:        config.CAFile,
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	authorizer := auth.New(
		config.ACLModelFile,
		config.ACLPolicyFile,
	)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := ioutil.TempFile("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: 1 * time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()
	//client = apiv1.NewLogClient(cc)
	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		cc.Close()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
		clog.Remove()
	}

}
