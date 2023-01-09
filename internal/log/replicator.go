package log

import (
	"context"
	"sync"

	apiv1 "github.com/araji/proglog/api/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer apiv1.LogClient

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		//already replicating to this server
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])
	return nil
}
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()
	client := apiv1.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx,
		&apiv1.ConsumeRequest{
			Offset: 0,
		})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}
	records := make(chan *apiv1.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx,
				&apiv1.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// lazy init of server map
// https://dave.cheney.net/2013/01/19/what-is-the-zero-value-and-why-is-it-useful
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
