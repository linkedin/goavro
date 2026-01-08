package netty

import (
	"testing"

	"bytes"
	"reflect"
	"io/ioutil"
	"runtime"
	"net"
	"github.com/sebglon/goavro/transceiver"
	"strconv"
	"io"
)

const (
	RECV_BUF_LEN = 1024
	NETWORK = "tcp"
	HOST = "127.0.0.1"
	PORT=6666
	ADDR="127.0.0.1:6666"
)


func init() {
	numProcs := runtime.NumCPU()
	if numProcs < 2 {
		numProcs = 2
	}
	runtime.GOMAXPROCS(numProcs)

	listener, err := net.Listen(NETWORK, "0.0.0.0:"+strconv.Itoa(PORT))
	if err != nil {
		println("error listening:", err.Error())
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				println("Error accept:", err.Error())
				return
			}
			go EchoFunc(conn)
		}
	}()
}

func EchoFunc(conn net.Conn) {
	for {
		buf := make([]byte, RECV_BUF_LEN)
		n, err := conn.Read(buf)
		if err != nil {
			println("Error reading:", err.Error())
			return
		}
		println("received ", n, " bytes of data =", string(buf))
		n, err = conn.Write(buf)
		if err != nil {
			println("Error writing:", err.Error())
			return
		}
		println("sended ", n, " bytes of data =", string(buf))
	}
}

func TestTransceive(t *testing.T) {
	f, err := NewTransceiver(transceiver.Config{Network:NETWORK, Host:HOST, Port:PORT})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.InitHandshake(func()([]byte, error){return make([]byte,1), nil},func(io.Reader)(bool, error){return true, nil})


	msg := "This is test writing."
	bmsg := make([]bytes.Buffer, 1)
	bmsg[0] = *bytes.NewBuffer([]byte(msg))

	resp, err := f.Transceive(bmsg)
	if err != nil {
		t.Fatal(err.Error())
	}
	brcv := make([]byte, len([]byte(msg)))
	resp[0].Read(brcv)
	rcv := string(brcv)
	if rcv != msg {
		t.Errorf("got %s, except %s", rcv, msg)
	}

}
func TestPack(t *testing.T) {
	transceiver := new(NettyTransceiver)
	frame := new(bytes.Buffer)
	transceiver.Pack(frame,*new([]bytes.Buffer))
	if frame.Len()!=8 {
		t.Fatalf("Frame not equals to 8: %x", frame.Len())
	}
	reflect.DeepEqual(frame.Bytes(), []byte("\x00\x00\x00\x01\x00\x00\x00\x00"))
	frame.Reset()

	requests:= make([]bytes.Buffer, 2)
	requests[0] = *bytes.NewBuffer([]byte("buf1"))
	requests[1] = *bytes.NewBuffer([]byte("buf2xxxx"))
	transceiver.Pack(frame, requests)
	expectedSize:= 8+ requests[0].Len()+4 + requests[1].Len() + 4
	if frame.Len()!= expectedSize {
		t.Fatalf("Frame not equals to %x: %x / %x",expectedSize,  frame.Len(), frame.Bytes())
	}
}

func TestUnpack(t *testing.T) {
	transceiver := new(NettyTransceiver)
	frame := []byte("\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x01\x0a\x00\x00\x00\x01\x0b")
	respons, err := transceiver.Unpack(frame)
	if err != nil {
		t.Fatalf("%v",err)
	}

	if  len(respons)!=2 {
		t.Fatalf("Number of reponse frame not equals %x / %x",2,  len(respons))
	}

	var resp1 []byte
	var resp2 []byte
	resp1, _  =ioutil.ReadAll(respons[0])
	respons[1].Read(resp2)
	if !reflect.DeepEqual(resp1, []byte("\x0a")) && !reflect.DeepEqual(resp2, []byte("\x0b")) {
		t.Fatalf("Reponse message not equals (0) %x/%x; (1) %x/%x","\x0a", respons[0], "\x0b", respons[1])
	}

}

