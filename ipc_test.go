package goavro

import (
	"testing"
	"net"

)

func TestRequestor(t *testing.T) {
	//t.SkipNow()
	rAddr, err := net.ResolveTCPAddr("tcp", "10.98.80.113:63001")
	conn, err := net.DialTCP("tcp", nil, rAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	transceiver := NewNettyTransceiver(conn)
	protocol, err := NewProtocol()
	if err != nil {
		t.Fatal(err)
	}

	flumeRecord, errFlume := protocol.getNewRecord("AvroFlumeEvent")
	if errFlume != nil {
		t.Fatal(errFlume)
	}
	headers := make(map[string]interface{})
	headers["host_header"] = "127.0.0.1"
	flumeRecord.Set("headers", headers)
	flumeRecord.Set("body", []byte("2016-08-02 14:45:38|flume.composantTechnique_IS_UNDEFINED|flume.application_IS_UNDEFINED|flume.client_IS_UNDEFINED|flume.plateforme_IS_UNDEFINED|instance_IS_UNDEFINED|logname_IS_UNDEFINED|WARN |test.LogGenerator|test !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"))
	requestor := NewRequestor(protocol, transceiver)
	err = requestor.Request("append", flumeRecord)

	if err != nil {
		t.Fatal("Request: ", err)
	}
}

