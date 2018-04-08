package rpcx

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Client struct {
	mutex            sync.Mutex     // protects following
	client           unsafe.Pointer // *rpc.Client, may be nil
	closeCanBeCalled bool           // whether Client.client.Close can be called by Client.Close, the purpose is to make Client.Close compatible with net/rpc.Client.Close
	closed           bool           // user has called Close

	dialOptions dialOptions
}

func (client *Client) getClient() *rpc.Client {
	return (*rpc.Client)(atomic.LoadPointer(&client.client))
}

func (client *Client) setClient(rpcClient *rpc.Client) {
	atomic.StorePointer(&client.client, unsafe.Pointer(rpcClient))
}

type dialOptions struct {
	network           string
	address           string
	timeout           time.Duration
	block             bool
	logger            Logger
	pingServiceMethod string
	pingInterval      time.Duration
	callInterceptor   CallInterceptor
	goInterceptor     GoInterceptor
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

func WithLogger(logger Logger) DialOption {
	return func(o *dialOptions) {
		if logger == nil {
			return
		}
		o.logger = logger
	}
}

func WithHeartbeat(pingServiceMethod string, interval time.Duration) DialOption {
	return func(o *dialOptions) {
		if pingServiceMethod == "" {
			return
		}
		if interval <= 0 {
			return
		}
		o.pingServiceMethod = pingServiceMethod
		o.pingInterval = interval
	}
}

type CallInterceptor func(ctx context.Context, serviceMethod string, args interface{}, reply interface{}, invoker CallInvoker) error
type CallInvoker func(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error

func WithCallInterceptor(interceptor CallInterceptor) DialOption {
	return func(o *dialOptions) {
		if interceptor == nil {
			return
		}
		o.callInterceptor = interceptor
	}
}

type GoInterceptor func(ctx context.Context, serviceMethod string, args interface{}, reply interface{}, invoker GoInvoker) *rpc.Call
type GoInvoker func(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) *rpc.Call

func WithGoInterceptor(interceptor GoInterceptor) DialOption {
	return func(o *dialOptions) {
		if interceptor == nil {
			return
		}
		o.goInterceptor = interceptor
	}
}

func Dial(network, address string, opts ...DialOption) (*Client, error) {
	var client Client
	opts = append(opts, withNetworkAddress(network, address))
	for _, opt := range opts {
		opt(&client.dialOptions)
	}
	if client.dialOptions.logger == nil {
		WithLogger(defaultLogger)(&client.dialOptions)
	}
	client.closeCanBeCalled = true

	if client.dialOptions.block {
		if err := client.resetConnection(); err != nil {
			return nil, err
		}
	} else {
		go func() {
			if err := client.resetConnection(); err != nil {
				client.dialOptions.logger.Errorf("[error][rpcx]: resetConnection: %s", err.Error())
				return
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
			client.dialOptions.logger.Errorf("[error][rpcx]: ping: %s", err.Error())
			continue
		}
		if err = client.resetConnection(); err != nil {
			client.dialOptions.logger.Errorf("[error][rpcx]: resetConnection: %s", err.Error())
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
	if rpcClient := client.getClient(); rpcClient != nil {
		client.closeCanBeCalled = false
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
	client.closeCanBeCalled = true
	client.setClient(rpc.NewClient(conn))
	return nil
}

func (client *Client) Close() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	client.closed = true
	rpcClient := client.getClient()
	if rpcClient == nil {
		return rpc.ErrShutdown
	}
	if !client.closeCanBeCalled {
		client.closeCanBeCalled = true // next time can be called, compatible with net/rpc.Client.Close
		return nil
	}
	return rpcClient.Close()
}

func (client *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	return client.CallContext(context.Background(), serviceMethod, args, reply)
}

func (client *Client) CallContext(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	if interceptor := client.dialOptions.callInterceptor; interceptor != nil {
		return interceptor(ctx, serviceMethod, args, reply, client.callContext)
	}
	return client.callContext(ctx, serviceMethod, args, reply)
}

func (client *Client) callContext(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	rpcClient := client.getClient()
	if rpcClient == nil {
		return rpc.ErrShutdown
	}
	if ctx == context.Background() {
		call := <-rpcClient.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1)).Done
		return call.Error
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
	if interceptor := client.dialOptions.goInterceptor; interceptor != nil {
		return interceptor(ctx, serviceMethod, args, reply, client.goContext)
	}
	return client.goContext(ctx, serviceMethod, args, reply)
}

func (client *Client) goContext(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) *rpc.Call {
	rpcClient := client.getClient()
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
	if ctx == context.Background() {
		return rpcClient.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1))
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

var defaultLogger Logger = (*logger)(log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Llongfile))

type Logger interface {
	Errorf(format string, v ...interface{})
}

type logger log.Logger

func (l *logger) Errorf(format string, v ...interface{}) {
	(*log.Logger)(l).Output(2, fmt.Sprintf(format, v...))
}
