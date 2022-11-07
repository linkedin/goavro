package main
import (
	"github.com/sebglon/goavro"
	"log"
	"github.com/sebglon/goavro/transceiver/netty"
	"time"
)

func main() {
	//t.SkipNow()
	transceiver,err := netty.NewTransceiver(netty.Config{AsyncConnect:false, NettyHost:"192.168.11.152"})
	if err != nil {
		log.Fatal(err)
	}
	protocol, err := goavro.NewProtocol()
	if err != nil {
		log.Fatal(err)
	}

	flumeRecord, errFlume := protocol.NewRecord("AvroFlumeEvent")
	if errFlume != nil {
		log.Fatal(errFlume)
	}
	headers := make(map[string]interface{})
	headers["host_header"] = "127.0.0.1"
	flumeRecord.Set("headers", headers)

	requestor := goavro.NewRequestor(protocol, transceiver)

	flumeRecord.Set("body", []byte("test 1"))
	err = requestor.Request("append", flumeRecord)

	if err != nil {
		log.Fatal("Request 1: ", err)
	}

	log.Printf("Test 1 OK")


	time.Sleep(5 * time.Second)
	flumeRecord.Set("body", []byte("test 2"))
	err = requestor.Request("append", flumeRecord)

	if err != nil {
		log.Fatal("Request 2: ", err)
	}
	log.Printf("Test 2 OK")

}

