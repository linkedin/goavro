package main
import (
	"github.com/sebglon/goavro"
	"net"
	"log"

)

func main() {
	//t.SkipNow()
	rAddr, err := net.ResolveTCPAddr("tcp", "10.98.80.113:63001")
	conn, err := net.DialTCP("tcp", nil, rAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	transceiver := goavro.NewNettyTransceiver(conn)
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
	flumeRecord.Set("body", []byte("2016-08-02 14:45:38|flume.composantTechnique_IS_UNDEFINED|flume.application_IS_UNDEFINED|flume.client_IS_UNDEFINED|flume.plateforme_IS_UNDEFINED|instance_IS_UNDEFINED|logname_IS_UNDEFINED|WARN |test.LogGenerator|test !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"))
	requestor := goavro.NewRequestor(protocol, transceiver)
	err = requestor.Request("append", flumeRecord)

	if err != nil {
		log.Fatal("Request: ", err)
	}

	err = requestor.Request("append", flumeRecord)

	if err != nil {
		log.Fatal("Request: ", err)
	}
}

