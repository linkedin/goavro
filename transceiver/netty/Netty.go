package netty

import (
	"bytes"
	"net"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
	"strconv"
	"math"
	"os"
)



const (
	defaultHost                   = "127.0.0.1"
	defaultNetwork                = "tcp"
	defaultSocketPath             = ""
	defaultPort                   = 63001
	defaultTimeout                = 3 * time.Second
	defaultBufferLimit            = 8 * 1024 * 1024
	defaultRetryWait              = 500
	defaultMaxRetry               = 13
	defaultReconnectWaitIncreRate = 1.5
)

type Config struct {
	NettyPort       int           `json:"netty_port"`
	NettyHost       string        `json:"netty_host"`
	NettyNetwork    string        `json:"netty_network"`
	NettySocketPath string        `json:"netty_socket_path"`
	Timeout          time.Duration `json:"timeout"`
	AsyncConnect     bool          `json:"async_connect"`
	BufferLimit      int           `json:"buffer_limit"`
	RetryWait        int           `json:"retry_wait"`
	MaxRetry         int           `json:"max_retry"`
}


type NettyTransceiver struct {
	Config
	Conn         io.ReadWriteCloser
	reconnecting bool
	mu           sync.Mutex
	pending      []byte
}
func NewTransceiver(config Config) (f* NettyTransceiver, err error){
	if config.NettyNetwork == "" {
		config.NettyNetwork = defaultNetwork
	}
	if config.NettyHost == "" {
		config.NettyHost = defaultHost
	}
	if config.NettyPort == 0 {
		config.NettyPort = defaultPort
	}
	if config.NettySocketPath == "" {
		config.NettySocketPath = defaultSocketPath
	}
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	if config.BufferLimit == 0 {
		config.BufferLimit = defaultBufferLimit
	}
	if config.RetryWait == 0 {
		config.RetryWait = defaultRetryWait
	}
	if config.MaxRetry == 0 {
		config.MaxRetry = defaultMaxRetry
	}
	if config.AsyncConnect {
		f = &NettyTransceiver{Config: config, reconnecting: true}
		f.reconnect()
	} else {
		f = &NettyTransceiver{Config: config, reconnecting: false}
		err = f.connect()
	}
	return
}

func (t NettyTransceiver) Transceive(requests []bytes.Buffer) ([]io.Reader, error){
	nettyFrame := new(bytes.Buffer)
	t.Pack(nettyFrame, requests)

	// Send request
	t.pending = append(t.pending, nettyFrame.Bytes()...)
	if err := t.send(); err != nil {
		t.close()
		if len(t.pending) > t.Config.BufferLimit {
			t.flushBuffer()
		}
	} else {
		t.flushBuffer()
	}

	// Read Response
	bodyBytes := make([]byte, 1024)
	 t.receive(bodyBytes)
//	if err!=nil {
//		return nil, fmt.Errorf("Fail to read on socket %v", err)
//	}
	return t.Unpack(bodyBytes)
}

func (t *NettyTransceiver) Pack(frame *bytes.Buffer, requests []bytes.Buffer) {
	// Set Netty Serial

	nettySerial :=make([]byte, 4)
	binary.BigEndian.PutUint32(nettySerial, uint32(1))
	frame.Write(nettySerial)


	nettySizeBuffer :=make([]byte, 4)
	binary.BigEndian.PutUint32(nettySizeBuffer, uint32(len(requests)))
	frame.Write(nettySizeBuffer)

	for _, request := range requests {
		requestSize :=make([]byte, 4)
		binary.BigEndian.PutUint32(requestSize, uint32(request.Len()))
		frame.Write(requestSize)
		frame.Write(request.Bytes())
	}
}

func (t *NettyTransceiver) Unpack(frame []byte) ([]io.Reader, error) {
	nettyNumberFame := binary.BigEndian.Uint32(frame[4:8])
	result := make([]io.Reader, nettyNumberFame)
	startFrame := uint32(8)
	i:=uint32(0)
	for i < nettyNumberFame  {
		messageSize := uint32(binary.BigEndian.Uint32(frame[startFrame:startFrame+4]))
		message := frame[startFrame+4:startFrame+4+messageSize]
		startFrame = startFrame+4+messageSize
		br := bytes.NewReader(message)
		result[i] = br
		i++
	}

	return  result, nil
}

// connect establishes a new connection using the specified transport.
func (f *NettyTransceiver) connect() (error) {
	var err error
	switch f.Config.NettyNetwork {
	case "tcp":
		f.Conn, err = net.DialTimeout(f.Config.NettyNetwork, f.Config.NettyHost+":"+strconv.Itoa(f.Config.NettyPort), f.Config.Timeout)
	case "unix":
		f.Conn, err = net.DialTimeout(f.Config.NettyNetwork, f.Config.NettySocketPath, f.Config.Timeout)
	default:
		err = net.UnknownNetworkError(f.Config.NettyNetwork)
	}
	return err
}


func e(x, y float64) int {
	return int(math.Pow(x, y))
}

func (f *NettyTransceiver) reconnect() {
	go func() {

		for i := 0; ; i++ {

			err := f.connect()
			if err == nil {
				f.mu.Lock()
				f.reconnecting = false
				f.mu.Unlock()
				break
			} else {
				if i == f.Config.MaxRetry {
					panic("Netty#reconnect: failed to reconnect!")
				}
				waitTime := f.Config.RetryWait * e(defaultReconnectWaitIncreRate, float64(i-1))
				time.Sleep(time.Duration(waitTime) * time.Millisecond)
			}
		}
	}()
}

func (f *NettyTransceiver) flushBuffer() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending = f.pending[0:0]
}


// Close closes the connection.
func (f *NettyTransceiver) Close() (err error) {
	if len(f.pending) > 0 {
		err = f.send()
	}
	f.close()
	return
}

// close closes the connection.
func (f *NettyTransceiver) close() (err error) {
	if f.Conn != nil {
		f.mu.Lock()
		defer f.mu.Unlock()
	} else {
		return
	}
	if f.Conn != nil {
		f.Conn.Close()
		f.Conn = nil
	}
	return
}

func (f *NettyTransceiver) send() (err error) {
	if f.Conn == nil {
		if f.reconnecting == false {
			f.mu.Lock()
			f.reconnecting = true
			f.mu.Unlock()
			f.reconnect()
		}
		err = fmt.Errorf("Netty#send: can't send logs, client is reconnecting")
	} else {
		f.mu.Lock()
		_, err = f.Conn.Write(f.pending)
		f.mu.Unlock()
	}
	return
}

func (f *NettyTransceiver) receive(resp []byte) (err error) {

	if f.Conn == nil {
		if f.reconnecting == false {
			f.mu.Lock()
			f.reconnecting = true
			f.mu.Unlock()
			f.reconnect()
		}
		err = fmt.Errorf("Netty#receive: can't send logs, client is reconnecting")
	} else {
		f.mu.Lock()
		_, err = f.Conn.Read(resp)
		f.mu.Unlock()
	}
	return
}