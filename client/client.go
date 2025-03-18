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
	Id                 uint64
	Address            string
	Self               *protocol.Connection
	ServerDecoders     []*gob.Decoder
	ServerEncoder      []*gob.Encoder
	WriteVersionVector []uint64
	ReadVersionVector  []uint64
	SessionSemantic    uint64
}

type Client struct {
	Id                 uint64
	NumberOfServers    uint64
	WriteVersionVector []uint64
	ReadVersionVector  []uint64
	SessionSemantic    uint64
	// Ack             bool
}

func New(id uint64, address string, sessionSemantic uint64, self *protocol.Connection, servers []*protocol.Connection) *NClient {
	i := uint64(0)
	serverDecoders := make([]*gob.Decoder, len(servers))
	serverEncoders := make([]*gob.Encoder, len(servers))

	for i < uint64(len(servers)) {
		c, err := net.Dial(servers[i].Network, servers[i].Address)
		fmt.Println(c.LocalAddr().String())
		if err != nil {
			fmt.Println(err)
		}
		serverDecoders[i] = gob.NewDecoder(c)
		serverEncoders[i] = gob.NewEncoder(c)
		i += 1
	}

	return &NClient{
		Id:                 id,
		Address:            address,
		Self:               self,
		ServerDecoders:     serverDecoders,
		ServerEncoder:      serverEncoders,
		WriteVersionVector: make([]uint64, len(servers)),
		ReadVersionVector:  make([]uint64, len(servers)),
		SessionSemantic:    sessionSemantic,
	}
}

func Start(c *NClient) error {
	// gob.Register(server.Message{})
	i := uint64(0)
	start := time.Now()
	for i < uint64(50000) {
		fmt.Println(i)
		v := uint64(rand.Int64())
		serverId := uint64(rand.Uint64() % uint64((len(c.ServerDecoders))))
		outGoingMessage := handler(c, 1, serverId, v, server.Message{})
		// fmt.Print(outGoingMessage, "\n")

		var m server.Message
		// fmt.Print(outGoingMessage, "\n")
		err := c.ServerEncoder[serverId].Encode(&outGoingMessage)
		if err != nil {
			fmt.Print(err)
			return err
		}
		err = c.ServerDecoders[serverId].Decode(&m)
		if err != nil {
			fmt.Print(err)
			return err
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

	for i < uint64(len(c.ServerDecoders)) {
		outGoingMessage := server.Message{MessageType: 4}
		err := c.ServerEncoder[i].Encode(&outGoingMessage)
		if err != nil {
			fmt.Print(err)
		}
		i++
	}

	for {

	}
}

func maxTwoInts(x uint64, y uint64) uint64 {
	if x > y {
		return x
	} else {
		return y
	}
}

func maxTS(t1 []uint64, t2 []uint64) []uint64 {
	var i = uint64(0)
	var length = uint64(len(t1))
	var output = make([]uint64, len(t1))
	for i < length {
		output[i] = maxTwoInts(t1[i], t2[i])
		i += 1
	}
	return output
}

func read(client Client, serverId uint64) server.Message {
	if client.SessionSemantic == 0 || client.SessionSemantic == 1 || client.SessionSemantic == 2 { // Eventual WFR MW
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 0,
			C2S_Client_Data:          0,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: make([]uint64, client.NumberOfServers),
		}
	} else if client.SessionSemantic == 3 { // MR
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 0,
			C2S_Client_Data:          0,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: client.ReadVersionVector,
		}
	} else if client.SessionSemantic == 4 { // RYW
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 0,
			C2S_Client_Data:          0,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: client.WriteVersionVector,
		}
	} else if client.SessionSemantic == 5 { // Causal
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 0,
			C2S_Client_Data:          0,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: maxTS(client.WriteVersionVector, client.ReadVersionVector),
		}
	}

	panic("The session semantic is not suported")
}

func write(client Client, serverId uint64, value uint64) server.Message {
	if client.SessionSemantic == 0 || client.SessionSemantic == 3 || client.SessionSemantic == 4 { // Eventual MR RYW
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          value,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: make([]uint64, client.NumberOfServers),
		}
	} else if client.SessionSemantic == 1 { // WFR
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          value,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: client.ReadVersionVector,
		}
	} else if client.SessionSemantic == 2 { // MW
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          value,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: client.WriteVersionVector,
		}
	} else if client.SessionSemantic == 5 { // Causal
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          value,
			C2S_Server_Id:            serverId,
			C2S_Client_VersionVector: maxTS(client.WriteVersionVector, client.ReadVersionVector),
		}
	}

	panic("The session semantic is not suported")
}

func processRequest(client Client, requestType uint64, serverId uint64, value uint64, ackMessage server.Message) (Client, server.Message) {
	if requestType == 0 {
		return client, read(client, serverId)
	} else if requestType == 1 {
		return client, write(client, serverId, value)
	} else if requestType == 2 {
		if ackMessage.S2C_Client_OperationType == 0 {
			client.ReadVersionVector = maxTS(client.ReadVersionVector, ackMessage.S2C_Client_VersionVector)
		}
		if ackMessage.S2C_Client_OperationType == 1 {
			client.WriteVersionVector = ackMessage.S2C_Client_VersionVector
		}
		return client, server.Message{}
	}

	panic("Unsupported request type")
}

func handler(c *NClient, requestType uint64, serverId uint64, value uint64, ackMessage server.Message) server.Message {
	nc, outGoingMessage := processRequest(Client{
		Id:                 c.Id,
		NumberOfServers:    uint64(len(c.ServerEncoder)),
		WriteVersionVector: c.WriteVersionVector,
		ReadVersionVector:  c.ReadVersionVector,
		SessionSemantic:    c.SessionSemantic,
	}, requestType, serverId, value, ackMessage)

	c.WriteVersionVector = nc.WriteVersionVector
	c.ReadVersionVector = nc.ReadVersionVector

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
