package client

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/alanwang67/session_semantics/protocol"
	"github.com/alanwang67/session_semantics/server"
)

type Client struct {
	Id            uint64
	Address       string
	Self          *protocol.Connection
	Servers       []*protocol.Connection
	VersionVector []uint64
	RequestNumber uint64
	Ack           bool
	mu            sync.Mutex
}

func New(id uint64, address string, self *protocol.Connection, servers []*protocol.Connection) *Client {
	return &Client{
		Id:            id,
		Address:       address,
		Self:          self,
		Servers:       servers,
		RequestNumber: 1,
		Ack:           true,
		VersionVector: make([]uint64, 0),
	}
}

func (c *Client) Start() error {

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
	for i < uint64(10000) {
		for c.Ack == false {
			c.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			c.mu.Lock()
		}
		c.mu.Unlock()
		// c.WriteToServer(rand.Uint64(), 0, 4)

		c.WriteToServer(0, uint64(i%uint64((len(c.Servers)))), 4)
		c.mu.Lock()
		fmt.Println(i)
		i++
	}
	c.mu.Unlock()

	// c.WriteToServer(rand.Uint64(), 0, 4)

	time.Sleep(1000 * time.Millisecond)

	i = uint64(0)

	for i < uint64(len(c.Servers)) {
		clientRequest := server.Message{}

		clientReply := server.Message{}

		h, _ := rpc.Dial(c.Servers[i].Network, c.Servers[i].Address)

		h.Call("RpcServer.PrintData", &clientRequest, &clientReply)

		i++
	}

	for {

	}
}

func (c *Client) AcknowledgeRequest(request *server.Message, reply *server.Message) error {
	c.mu.Lock()

	fmt.Print("Acknowledged")

	if request.S2C_Client_Succeeded {
		c.VersionVector = request.S2C_Client_VersionVector
		c.RequestNumber = max(c.RequestNumber, request.S2C_Client_RequestNumber+1)
	}

	c.Ack = true
	c.mu.Unlock()

	return nil
}

// func write(client Client, value uint64, serverId uint64, sessionSemantic uint64) server.Message {

// }

func (c *Client) WriteToServer(value uint64, serverId uint64, sessionSemantic uint64) error {
	c.mu.Lock()
	c.Ack = false

	if sessionSemantic == 0 || sessionSemantic == 1 { // WFR MW
		clientRequest := server.Message{
			MessageType:              0,
			C2S_Client_Id:            c.Id,
			C2S_Client_RequestNumber: c.RequestNumber,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          10,
			C2S_Client_VersionVector: c.VersionVector,
		}
		protocol.Invoke(*c.Servers[serverId], "RpcServer.RpcHandler", &clientRequest, &server.Message{})
	} else {
		clientRequest := server.Message{
			C2S_Client_Id:            c.Id,
			C2S_Client_RequestNumber: c.RequestNumber,
			C2S_Client_OperationType: 1,
			C2S_Client_Data:          10,
			C2S_Client_VersionVector: make([]uint64, len(c.Servers)),
		}
		protocol.Invoke(*c.Servers[serverId], "RpcServer.RpcHandler", &clientRequest, &server.Message{})
	}
	c.mu.Unlock()

	return nil
}
