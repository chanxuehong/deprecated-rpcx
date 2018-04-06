package rpcx

import (
	"context"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Client struct {
	mutex            sync.Mutex     // protects following
	rpcClient        unsafe.Pointer // *rpc.Client, may be nil
	lastClosedClient *rpc.Client    // last rpc.Client closed by Client.resetConnection
	closed           bool           // user has called Close

	dialOptions dialOptions
}

func (client *Client) getRpcClient() *rpc.Client {
	return (*rpc.Client)(atomic.LoadPointer(&client.rpcClient))
}

func (client *Client) setRpcClient(rpcClient *rpc.Client) {
	atomic.StorePointer(&client.rpcClient, unsafe.Pointer(rpcClient))
}

type dialOptions struct {
	network           string
	address           string
	timeout           time.Duration
	block             bool
	pingServiceMethod string
	pingInterval      time.Duration
}

type DialOption func(*dialOptions)

func withNetworkAddress(network, address string) DialOption {
	return func(o *dialOptions) {
		o.network = network
		o.address = address
	}
}

func WithTimeout(d time.Duration) DialOption {
	return func(o *dialOptions) {
		if d <= 0 {
			return
		}
		o.timeout = d
	}
}

func WithBlock() DialOption {
	return func(o *dialOptions) {
		o.block = true
	}
}

func WithHeartbeat(serviceMethod string, interval time.Duration) DialOption {
	return func(o *dialOptions) {
		if serviceMethod == "" {
			return
		}
		if interval <= 0 {
			return
		}
		o.pingServiceMethod = serviceMethod
		o.pingInterval = interval
	}
}

func Dial(network, address string, opts ...DialOption) (*Client, error) {
	var client Client
	opts = append(opts, withNetworkAddress(network, address))
	for _, opt := range opts {
		opt(&client.dialOptions)
	}

	if client.dialOptions.block {
		if err := client.resetConnection(); err != nil {
			return nil, err
		}
	} else {
		go func() {
			if err := client.resetConnection(); err != nil {
				log.Println("rpc: resetConnection failed", err)
			}
		}()
	}
	if client.dialOptions.pingServiceMethod != "" && client.dialOptions.pingInterval > 0 {
		go client.monitor()
	}
	return &client, nil
}

func (client *Client) monitor() {
	ticker := time.NewTicker(client.dialOptions.pingInterval)
	defer ticker.Stop()

	var (
		closed bool
		err    error
	)
	for range ticker.C {
		client.mutex.Lock()
		closed = client.closed
		client.mutex.Unlock()
		if closed {
			return
		}
		err = client.ping()
		if err == nil {
			continue
		}
		if err != rpc.ErrShutdown {
			log.Println("rpc: ping failed", err)
			continue
		}
		if err = client.resetConnection(); err != nil {
			log.Println("rpc: resetConnection failed", err)
			continue
		}
	}
}

func (client *Client) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var args, reply struct{}
	return client.CallContext(ctx, client.dialOptions.pingServiceMethod, &args, &reply)
}

func (client *Client) resetConnection() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.closed {
		return nil
	}
	if rpcClient := client.getRpcClient(); rpcClient != nil {
		client.lastClosedClient = rpcClient
		if err := rpcClient.Close(); err != nil && err != rpc.ErrShutdown {
			return err
		}
	}
	dialer := net.Dialer{
		Timeout:   client.dialOptions.timeout,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}
	conn, err := dialer.Dial(client.dialOptions.network, client.dialOptions.address)
	if err != nil {
		return err
	}
	client.setRpcClient(rpc.NewClient(conn))
	return nil
}

func (client *Client) Close() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	client.closed = true
	if rpcClient := client.getRpcClient(); rpcClient != nil && rpcClient != client.lastClosedClient {
		return rpcClient.Close()
	}
	return nil
}

func (client *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	return client.CallContext(context.Background(), serviceMethod, args, reply)
}

func (client *Client) CallContext(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	rpcClient := client.getRpcClient()
	if rpcClient == nil {
		return rpc.ErrShutdown
	}
	select {
	case call := <-rpcClient.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1)).Done:
		return call.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (client *Client) Go(serviceMethod string, args interface{}, reply interface{}) *rpc.Call {
	return client.GoContext(context.Background(), serviceMethod, args, reply)
}

func (client *Client) GoContext(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) *rpc.Call {
	rpcClient := client.getRpcClient()
	if rpcClient == nil {
		call := &rpc.Call{
			ServiceMethod: serviceMethod,
			Args:          args,
			Reply:         reply,
			Error:         rpc.ErrShutdown,
			Done:          make(chan *rpc.Call, 1), // buffered.
		}
		call.Done <- call
		return call
	}
	done := make(chan *rpc.Call, 1) // buffered.
	go func() {
		done <- rpcClient.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1))
	}()
	select {
	case call := <-done:
		return call
	case <-ctx.Done():
		call := &rpc.Call{
			ServiceMethod: serviceMethod,
			Args:          args,
			Reply:         reply,
			Error:         ctx.Err(),
			Done:          make(chan *rpc.Call, 1), // buffered.
		}
		call.Done <- call
		return call
	}
}
