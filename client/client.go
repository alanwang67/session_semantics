package client

import (
	"encoding/gob"
	"fmt"
	"math/rand/v2"
	"net"
	"time"

	"github.com/alanwang67/session_semantics/protocol"
	"github.com/alanwang67/session_semantics/server"
)

type NClient struct {
	Id              uint64
	Address         string
	Self            *protocol.Connection
	Servers         []net.Conn
	VersionVector   []uint64
	RequestNumber   uint64
	SessionSemantic uint64
	// Ack             bool
}

type Client struct {
	Id              uint64
	NumberOfServers uint64
	VersionVector   []uint64
	SessionSemantic uint64
	// Ack             bool
}

func New(id uint64, address string, sessionSemantic uint64, self *protocol.Connection, servers []*protocol.Connection) *NClient {
	i := uint64(0)
	serversConn := make([]net.Conn, len(servers))

	// is this okay since we are assuming all servers are running
	for i < uint64(len(servers)) {
		c, err := net.Dial(servers[i].Network, servers[i].Address)
		if err != nil {
			fmt.Println(err)
		}
		serversConn[i] = c
		i += 1
	}

	return &NClient{
		Id:              id,
		Address:         address,
		Self:            self,
		Servers:         serversConn,
		SessionSemantic: sessionSemantic,
		VersionVector:   make([]uint64, len(servers)),
	}
}

func Start(c *NClient) error {
	i := uint64(0)
	start := time.Now()
	for i < uint64(100000) {
		v := uint64(rand.Int64())
		fmt.Println(i)
		// fmt.Print(v, "\n")

		// fmt.Println("Write Value: ", v)
		// read request: uint64(rand.Uint64()%uint64(2))
		serverId := uint64(rand.Uint64() % uint64((len(c.Servers))))
		outGoingMessage := handler(c, 1, serverId, v, server.Message{})
		// fmt.Print(outGoingMessage, "\n")

		var m server.Message
		enc := gob.NewEncoder(c.Servers[serverId])
		dec := gob.NewDecoder(c.Servers[serverId])
		// fmt.Print(outGoingMessage, "\n")
		err := enc.Encode(&outGoingMessage)
		if err != nil {
			fmt.Print(err)
		}

		err = dec.Decode(&m)
		if err != nil {
			fmt.Print(err)
			// return err
		}
		// once we get a response back we need to update our acknowledgement?
		handler(c, 2, 0, 0, m)
		i++
	}
	t := time.Now()
	fmt.Print(t.Sub(start))
	fmt.Print("done")
	time.Sleep(1000 * time.Millisecond)

	i = uint64(0)

	for i < uint64(len(c.Servers)) {
		outGoingMessage := server.Message{MessageType: 4}

		enc := gob.NewEncoder(c.Servers[i])
		err := enc.Encode(&outGoingMessage)
		if err != nil {
			fmt.Print(err)
		}
		i++
	}

	for {

	}
}

func write(client Client, serverId uint64, value uint64) server.Message {
	if client.SessionSemantic == 0 || client.SessionSemantic == 1 || client.SessionSemantic == 4 { // WFR MW Causal
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          value,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: client.VersionVector,
		}
	} else {
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          value,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: make([]uint64, client.NumberOfServers),
		}
	}
}

func read(client Client, serverId uint64) server.Message {
	if client.SessionSemantic == 2 || client.SessionSemantic == 3 || client.SessionSemantic == 4 { // MR RYW Causal
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 0,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: client.VersionVector,
		}
	} else {
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 0,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: make([]uint64, client.NumberOfServers),
		}
	}
}

func processRequest(client Client, requestType uint64, serverId uint64, value uint64, ackMessage server.Message) (Client, server.Message) {
	if requestType == 0 {
		return client, read(client, serverId)
		// client.Ack = false
		// return client, write(client, serverId, value)
	} else if requestType == 1 {
		return client, write(client, serverId, value)
		// client.Ack = false
	} else if requestType == 2 {
		//
		if ackMessage.S2C_Client_OperationType == 0 && (client.SessionSemantic == 2 || client.SessionSemantic == 3 || client.SessionSemantic == 4) {
			client.VersionVector = ackMessage.S2C_Client_VersionVector
		}

		if ackMessage.S2C_Client_OperationType == 1 && (client.SessionSemantic == 0 || client.SessionSemantic == 1 || client.SessionSemantic == 4) {
			client.VersionVector = ackMessage.S2C_Client_VersionVector
		}
		return client, server.Message{}
	}

	panic("Unsupported request type")
}

func handler(c *NClient, requestType uint64, serverId uint64, value uint64, ackMessage server.Message) server.Message {
	nc, outGoingMessage := processRequest(Client{
		Id:              c.Id,
		NumberOfServers: uint64(len(c.Servers)),
		VersionVector:   c.VersionVector,
		SessionSemantic: c.SessionSemantic,
	}, requestType, serverId, value, ackMessage)

	c.VersionVector = nc.VersionVector

	return outGoingMessage
}

// func (c *RpcClient) AcknowledgeMessage(request *server.Message, reply *server.Message) error {
// 	c.mu.Lock()
// 	// if request.MessageType == 4 && request.S2C_Client_OperationType == 0 {
// 	// 	fmt.Println("Read value: ", request.S2C_Client_Data)
// 	// }
// 	nc, _ := processRequest(Client{
// 		Id:              c.Id,
// 		NumberOfServers: uint64(len(c.Servers)),
// 		VersionVector:   c.VersionVector,
// 		SessionSemantic: c.SessionSemantic,
// 		Ack:             c.Ack}, 2, 0, 0, *request)

// 	c.VersionVector = nc.VersionVector
// 	c.Ack = nc.Ack

// 	c.mu.Unlock()

// 	return nil
// }
