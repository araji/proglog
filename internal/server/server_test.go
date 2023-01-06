package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	apiv1 "github.com/araji/proglog/api/v1"

	"github.com/araji/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client apiv1.LogClient,
		config *Config,
	){
		"produce and consume a record":  testProduceConsume,
		"produce and stream of records": testProduceConsumeStream,
		"consume out of range error":    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func testProduceConsume(t *testing.T, client apiv1.LogClient, config *Config) {
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
func testProduceConsumeStream(t *testing.T, client apiv1.LogClient, config *Config) {
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
func testConsumePastBoundary(t *testing.T, client apiv1.LogClient, config *Config) {

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

func setupTest(t *testing.T, fn func(*Config)) (client apiv1.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

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

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()
	client = apiv1.NewLogClient(cc)
	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}

}
