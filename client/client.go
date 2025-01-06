package client

import (
	"fmt"
	"math/rand/v2"
	"net/rpc"
	"time"

	"github.com/alanwang67/session_semantics/server"
)

type Client struct {
	Id          uint64
	Servers     []*server.Connection
	ReadVector  []uint64
	WriteVector []uint64
}

func New(id uint64, servers []*server.Connection) *Client {
	return &Client{
		Id:          id,
		Servers:     servers,
		ReadVector:  make([]uint64, len(servers)),
		WriteVector: make([]uint64, len(servers)),
	}
}

func (c *Client) WriteToServer(value uint64, serverId uint64, sessionSemantic uint64) uint64 {
	i := uint64(0)
	l := uint64(10)

	for i < l {
		clientRequest := server.Request{
			Client_OperationType: 1,
			Client_SessionType:   sessionSemantic,
			Client_Data:          value,
			Client_ReadVector:    c.ReadVector,
			Client_WriteVector:   c.WriteVector,
		}

		clientReply := server.Reply{}

		h, _ := rpc.Dial(c.Servers[serverId].Network, c.Servers[serverId].Address)

		h.Call("Server.ProcessRequest", &clientRequest, &clientReply)

		if clientReply.Client_Succeeded {
			c.ReadVector = clientReply.Client_ReadVector
			c.WriteVector = clientReply.Client_WriteVector
			return clientReply.Client_Data
		}

		i++
	}

	panic("We can not serve your request")
}

func (c *Client) Start() error {
	i := uint64(0)
	for i < uint64(100) {

		c.WriteToServer(rand.Uint64(), uint64(i%uint64((len(c.Servers)))), 0)

		i++
	}

	time.Sleep(100 * time.Millisecond)

	i = uint64(0)
	fmt.Println(len(c.Servers))
	for i < uint64(len(c.Servers)) {
		clientRequest := server.Request{}

		clientReply := server.Reply{}

		h, _ := rpc.Dial(c.Servers[i].Network, c.Servers[i].Address)

		h.Call("Server.PrintData", &clientRequest, &clientReply)

		i++
	}

	for {

	}
}
