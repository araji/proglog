package server

import (
	"context"

	apiv1 "github.com/araji/proglog/api/v1"
	"google.golang.org/grpc"
)

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	apiv1.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// CommitLog is the interface that wraps the basic Append and Read methods.
// abstract log to allow using in-memory for testing and persistent for production
type CommitLog interface {
	Append(*apiv1.Record) (uint64, error)
	Read(uint64) (*apiv1.Record, error)
}
type Config struct {
	CommitLog CommitLog
}

var _ apiv1.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	apiv1.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *apiv1.ProduceRequest) (*apiv1.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &apiv1.ProduceResponse{Offset: offset}, nil
}
func (s *grpcServer) Consume(ctx context.Context, req *apiv1.ConsumeRequest) (*apiv1.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &apiv1.ConsumeResponse{Record: record}, nil
}
func (s *grpcServer) ProduceStream(
	stream apiv1.Log_ProduceStreamServer,
) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}
func (s *grpcServer) ConsumeStream(req *apiv1.ConsumeRequest, stream apiv1.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case apiv1.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err := stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
