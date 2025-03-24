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

func New(id uint64, self *protocol.Connection, sessionSemantic uint64, servers []*protocol.Connection) *NClient {
	i := uint64(0)
	serverDecoders := make([]*gob.Decoder, len(servers))
	serverEncoders := make([]*gob.Encoder, len(servers))

	for i < uint64(len(servers)) {
		c, err := net.Dial(servers[i].Network, servers[i].Address)
		// fmt.Println(c.LocalAddr().String())
		if err != nil {
			fmt.Println(err)
		}
		serverDecoders[i] = gob.NewDecoder(c)
		serverEncoders[i] = gob.NewEncoder(c)
		i += 1
	}

	return &NClient{
		Id:                 id,
		Self:               self,
		ServerDecoders:     serverDecoders,
		ServerEncoder:      serverEncoders,
		WriteVersionVector: make([]uint64, len(servers)),
		ReadVersionVector:  make([]uint64, len(servers)),
		SessionSemantic:    sessionSemantic,
	}
}

// len(servers) is random for pinnedServer
func Start(clients []*protocol.Connection, pinnedServer []uint64, sessionSemantics []uint64, servers []*protocol.Connection) error {
	i := uint64(0)

	var NClients = make([]*NClient, len(clients))

	for i < uint64(len(clients)) {
		NClients[i] = New(i, clients[i], sessionSemantics[i], servers)
		i += 1
	}

	i = uint64(0)
	for i < uint64(len(NClients)) {
		j := i
		go func(c *NClient) error {
			fmt.Println(c)
			// make barrier here bc creating threads take time, discard first 10% and last 10%
			// after the first 10k take time, then at the last 10k take time
			// sum of requests / time
			// average latency?
			// this is just 1 point bash script (number of points should saturate -> 1, 2, 4, 8, 16 ... 512
			// script that plots this
			// each experiment runs for at least 1 minute
			// ycsb benchmark? read - 50, write - 50
			// primaryback up vs gossip? (self-comparison) necessary
			// comparison of different semantics with primary-backup

			// primary back up (write to primary, pin client to a backup) 1 primary 2 backups vs gossip first
			// 50% write 50% read ziffian
			// go run vs binary?
			// temux

			start := time.Now()
			index := uint64(0)
			for index < uint64(10000) {
				fmt.Println(index)

				// if index == 2000 {
				// 	// set the start time here
				// }

				// if index == {
				// 	// close the start time here
				// }

				v := uint64(rand.Int64())
				serverId := uint64(rand.Uint64() % uint64((len(c.ServerDecoders))))
				outGoingMessage := handler(c, 1, serverId, v, server.Message{})

				var m server.Message
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
				handler(c, 2, 0, 0, m)
				index++
			}
			t := time.Now()
			fmt.Print(t.Sub(start))

			time.Sleep(1000 * time.Millisecond)

			// put this in a wait group
			index = uint64(0)
			for index < uint64(len(c.ServerDecoders)) {
				outGoingMessage := server.Message{MessageType: 4}
				err := c.ServerEncoder[index].Encode(&outGoingMessage)
				if err != nil {
					fmt.Print(err)
				}
				index++
			}

			return nil
		}(NClients[j])

		i += 1
	}

	// we can add a wait group here when all the threads are done to print

	// fmt.Println("Done")

	// make sure main thead thread doesn't die before go routines return
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
	var reply = server.Message{}
	if client.SessionSemantic == 0 || client.SessionSemantic == 1 || client.SessionSemantic == 2 { // Eventual WFR MW
		reply.MessageType = 0
		reply.C2S_Client_Id = client.Id
		reply.C2S_Client_OperationType = 0
		reply.C2S_Client_Data = 0
		reply.C2S_Server_Id = serverId
		reply.C2S_Client_VersionVector = make([]uint64, client.NumberOfServers)
	} else if client.SessionSemantic == 3 { // MR
		reply.MessageType = 0
		reply.C2S_Client_Id = client.Id
		reply.C2S_Client_OperationType = 0
		reply.C2S_Client_Data = 0
		reply.C2S_Server_Id = serverId
		reply.C2S_Client_VersionVector = client.ReadVersionVector
	} else if client.SessionSemantic == 4 { // RYW
		reply.MessageType = 0
		reply.C2S_Client_Id = client.Id
		reply.C2S_Client_OperationType = 0
		reply.C2S_Client_Data = 0
		reply.C2S_Server_Id = serverId
		reply.C2S_Client_VersionVector = client.WriteVersionVector
	} else if client.SessionSemantic == 5 { // Causal
		reply.MessageType = 0
		reply.C2S_Client_Id = client.Id
		reply.C2S_Client_OperationType = 0
		reply.C2S_Client_Data = 0
		reply.C2S_Server_Id = serverId
		reply.C2S_Client_VersionVector = maxTS(client.WriteVersionVector, client.ReadVersionVector)
	}

	return reply
}

func write(client Client, serverId uint64, value uint64) server.Message {
	var reply = server.Message{}
	if client.SessionSemantic == 0 || client.SessionSemantic == 3 || client.SessionSemantic == 4 { // Eventual MR RYW
		reply.MessageType = 0
		reply.C2S_Client_Id = client.Id
		reply.C2S_Client_OperationType = 1
		reply.C2S_Client_Data = value
		reply.C2S_Server_Id = serverId
		reply.C2S_Client_VersionVector = make([]uint64, client.NumberOfServers)
	} else if client.SessionSemantic == 1 { // WFR
		reply.MessageType = 0
		reply.C2S_Client_Id = client.Id
		reply.C2S_Client_OperationType = 1
		reply.C2S_Client_Data = value
		reply.C2S_Server_Id = serverId
		reply.C2S_Client_VersionVector = client.ReadVersionVector
	} else if client.SessionSemantic == 2 { // MW
		reply.MessageType = 0
		reply.C2S_Client_Id = client.Id
		reply.C2S_Client_OperationType = 1
		reply.C2S_Client_Data = value
		reply.C2S_Server_Id = serverId
		reply.C2S_Client_VersionVector = client.WriteVersionVector
	} else if client.SessionSemantic == 5 { // Causal
		reply.MessageType = 0
		reply.C2S_Client_Id = client.Id
		reply.C2S_Client_OperationType = 1
		reply.C2S_Client_Data = value
		reply.C2S_Server_Id = serverId
		reply.C2S_Client_VersionVector = maxTS(client.WriteVersionVector, client.ReadVersionVector)
	}

	return reply
}

func processRequest(client Client, requestType uint64, serverId uint64, value uint64, ackMessage server.Message) (Client, server.Message) {
	var msg = server.Message{}
	if requestType == 0 {
		msg = read(client, serverId)
	} else if requestType == 1 {
		msg = write(client, serverId, value)
	} else if requestType == 2 {
		if ackMessage.S2C_Client_OperationType == 0 {
			client.ReadVersionVector = ackMessage.S2C_Client_VersionVector
		}
		if ackMessage.S2C_Client_OperationType == 1 {
			client.WriteVersionVector = ackMessage.S2C_Client_VersionVector
		}
		return client, server.Message{}
	}

	return client, msg
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
