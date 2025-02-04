package client

import (
	"fmt"
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
	RequestNumber   uint64
	Ack             bool
}

func New(id uint64, address string, sessionSemantic uint64, self *protocol.Connection, servers []*protocol.Connection) *RpcClient {
	return &RpcClient{
		Id:              id,
		Address:         address,
		Self:            self,
		Servers:         servers,
		RequestNumber:   1, //needs to be 1
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
	for i < uint64(100) {
		for !c.Ack {
			c.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			c.mu.Lock()
		}
		c.mu.Unlock()
		// c.WriteToServer(rand.Uint64(), 0, 4)

		c.communicateWithServer(1, uint64(rand.Uint64()%uint64((len(c.Servers)))), rand.Uint64())
		c.mu.Lock()
		fmt.Println(i)
		fmt.Println(c.VersionVector)
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

		c.communicateWithServer(0, i, rand.Uint64())

		i++
	}

	for {

	}
}

func (c *RpcClient) AcknowledgeRequest(request *server.Message, reply *server.Message) error {
	// fmt.Print("We got it")
	c.mu.Lock()

	if request.S2C_Client_OperationType == 0 {
		if c.SessionSemantic == 2 || c.SessionSemantic == 3 || c.SessionSemantic == 4 {
			c.VersionVector = request.S2C_Client_VersionVector
		}
	}

	if request.S2C_Client_OperationType == 1 {
		if c.SessionSemantic == 0 || c.SessionSemantic == 1 || c.SessionSemantic == 4 {
			c.VersionVector = request.S2C_Client_VersionVector
		}
	}

	// I'm not sure if we need to do the max?, depends on if there are acknowledgements that are skipped over
	c.RequestNumber = max(c.RequestNumber, request.S2C_Client_RequestNumber+1)
	c.Ack = true

	c.mu.Unlock()

	return nil
}

func write(client Client, value uint64) server.Message {
	if client.SessionSemantic == 0 || client.SessionSemantic == 1 || client.SessionSemantic == 4 { // WFR MW Causal
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_RequestNumber: client.RequestNumber,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          value,
			C2S_Client_VersionVector: client.VersionVector,
		}
	} else {
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_RequestNumber: client.RequestNumber,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          10,
			C2S_Client_VersionVector: make([]uint64, client.NumberOfServers),
		}
	}
}

func read(client Client) server.Message {
	if client.SessionSemantic == 2 || client.SessionSemantic == 3 || client.SessionSemantic == 4 { // MR RYW Causal
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_RequestNumber: client.RequestNumber,
			C2S_Client_OperationType: 0,
			C2S_Client_VersionVector: client.VersionVector,
		}
	} else {
		return server.Message{
			MessageType:              0,
			C2S_Client_Id:            client.Id,
			C2S_Client_RequestNumber: client.RequestNumber,
			C2S_Client_OperationType: 0,
			C2S_Client_VersionVector: make([]uint64, client.NumberOfServers),
		}
	}
}

func (c *RpcClient) communicateWithServer(operationType uint64, serverId uint64, value uint64) {
	c.mu.Lock()
	c.Ack = false

	if operationType == 0 {
		client := Client{
			Id:              c.Id,
			NumberOfServers: uint64(len(c.Servers)),
			VersionVector:   c.VersionVector,
			SessionSemantic: c.SessionSemantic,
			RequestNumber:   c.RequestNumber,
			Ack:             c.Ack,
		}
		message := read(client)
		protocol.Invoke(*c.Servers[serverId], "RpcServer.RpcHandler", &message, &server.Message{})
	} else {
		client := Client{
			Id:              c.Id,
			NumberOfServers: uint64(len(c.Servers)),
			VersionVector:   c.VersionVector,
			SessionSemantic: c.SessionSemantic,
			RequestNumber:   c.RequestNumber,
			Ack:             c.Ack,
		}
		message := write(client, value)
		fmt.Print(message)
		protocol.Invoke(*c.Servers[serverId], "RpcServer.RpcHandler", &message, &server.Message{})
	}

	c.mu.Unlock()

}
