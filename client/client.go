package client

import (
	"math/rand/v2"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/alanwang67/session_semantics/protocol"
	"github.com/alanwang67/session_semantics/server"
)

type RpcClient struct {
	Id              uint64
	Address         string
	Self            *protocol.Connection
	Servers         []*protocol.Connection
	VersionVector   []uint64
	RequestNumber   uint64
	SessionSemantic uint64
	Ack             bool
	mu              sync.Mutex
}

type Client struct {
	Id              uint64
	NumberOfServers uint64
	VersionVector   []uint64
	SessionSemantic uint64
	Ack             bool
}

func New(id uint64, address string, sessionSemantic uint64, self *protocol.Connection, servers []*protocol.Connection) *RpcClient {
	return &RpcClient{
		Id:              id,
		Address:         address,
		Self:            self,
		Servers:         servers,
		SessionSemantic: sessionSemantic,
		Ack:             true,
		VersionVector:   make([]uint64, len(servers)),
	}
}

func (c *RpcClient) Start() error {

	l, _ := net.Listen(c.Self.Network, c.Self.Address)
	defer l.Close()

	rpc.Register(c)

	go func() {
		for {
			rpc.Accept(l)
		}
	}()

	i := uint64(0)
	c.mu.Lock()
	for i < uint64(1000) {
		// fmt.Println(i)
		for !c.Ack {
			c.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
			c.mu.Lock()
		}
		c.mu.Unlock()

		v := uint64(rand.Int64())
		// fmt.Println("Write Value: ", v)
		c.requestHandler(uint64(rand.Uint64()%uint64(2)), uint64(rand.Uint64()%uint64((len(c.Servers)))), v, server.Message{})
		c.mu.Lock()
		i++
	}
	c.mu.Unlock()

	time.Sleep(1000 * time.Millisecond)

	i = uint64(0)

	for i < uint64(len(c.Servers)) {
		clientRequest := server.Message{}

		clientReply := server.Message{}

		h, _ := rpc.Dial(c.Servers[i].Network, c.Servers[i].Address)

		h.Call("RpcServer.PrintData", &clientRequest, &clientReply)

		c.requestHandler(0, i, 0, server.Message{})

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
		client.Ack = false
		return client, write(client, serverId, value)
	} else if requestType == 1 {
		client.Ack = false
		return client, read(client, serverId)
	} else if requestType == 2 {
		if ackMessage.S2C_Client_OperationType == 0 {
			if client.SessionSemantic == 2 || client.SessionSemantic == 3 || client.SessionSemantic == 4 {
				client.VersionVector = ackMessage.S2C_Client_VersionVector
			}
		}

		if ackMessage.S2C_Client_OperationType == 1 {
			if client.SessionSemantic == 0 || client.SessionSemantic == 1 || client.SessionSemantic == 4 {
				client.VersionVector = ackMessage.S2C_Client_VersionVector
			}
		}
		client.Ack = true
		return client, server.Message{}
	}

	panic("Unsupported request type")
}

func (c *RpcClient) requestHandler(requestType uint64, serverId uint64, value uint64, ackMessage server.Message) {
	c.mu.Lock()

	nc, outGoingMessage := processRequest(Client{
		Id:              c.Id,
		NumberOfServers: uint64(len(c.Servers)),
		VersionVector:   c.VersionVector,
		SessionSemantic: c.SessionSemantic,
		Ack:             c.Ack}, requestType, serverId, value, ackMessage)

	c.Ack = nc.Ack

	c.mu.Unlock()

	protocol.Invoke(*c.Servers[serverId], "RpcServer.RpcHandler", &outGoingMessage, &server.Message{})
}

func (c *RpcClient) AcknowledgeMessage(request *server.Message, reply *server.Message) error {
	c.mu.Lock()
	// if request.MessageType == 4 && request.S2C_Client_OperationType == 0 {
	// 	fmt.Println("Read value: ", request.S2C_Client_Data)
	// }
	nc, _ := processRequest(Client{
		Id:              c.Id,
		NumberOfServers: uint64(len(c.Servers)),
		VersionVector:   c.VersionVector,
		SessionSemantic: c.SessionSemantic,
		Ack:             c.Ack}, 2, 0, 0, *request)

	c.VersionVector = nc.VersionVector
	c.Ack = nc.Ack

	c.mu.Unlock()

	return nil
}
