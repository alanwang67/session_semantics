package client

import (
	"encoding/gob"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/alanwang67/session_semantics/protocol"
	"github.com/alanwang67/session_semantics/server"
)

type NClient struct {
	Id                 uint64
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
}

func New(id uint64, sessionSemantic uint64, servers []*protocol.Connection) *NClient {
	i := uint64(0)
	serverDecoders := make([]*gob.Decoder, len(servers))
	serverEncoders := make([]*gob.Encoder, len(servers))

	for i < uint64(len(servers)) {
		c, err := net.Dial(servers[i].Network, servers[i].Address)

		if err != nil {
			fmt.Println(err)
		}
		serverDecoders[i] = gob.NewDecoder(c)
		serverEncoders[i] = gob.NewEncoder(c)
		i += 1
	}

	return &NClient{
		Id:                 id,
		ServerDecoders:     serverDecoders,
		ServerEncoder:      serverEncoders,
		WriteVersionVector: make([]uint64, len(servers)),
		ReadVersionVector:  make([]uint64, len(servers)),
		SessionSemantic:    sessionSemantic,
	}
}

func Start(threads uint64, sessionSemantics []uint64, workload [][]uint64, pinnedWriteServer []uint64, pinnedReadServer []uint64, servers []*protocol.Connection) error {
        fmt.Println(sessionSemantics)
	i := uint64(0)

	var NClients = make([]*NClient, threads)
	numberOfOperations := len(workload[0])

	for i < uint64(threads) {
		NClients[i] = New(i, sessionSemantics[i], servers)
		i += 1
	}
	lower_bound := uint64(float64(numberOfOperations) * .2)
	upper_bound := uint64(float64(numberOfOperations) * .8)

	ops := threads * uint64(upper_bound-lower_bound)

	var l sync.Mutex

	set := false
	avg_time := float64(0)
	// avg_throughput := float64(0)
	total_latency := time.Duration(0 * time.Microsecond)

	var wg sync.WaitGroup
	var barrier sync.WaitGroup

	wg.Add(len(NClients))
	barrier.Add(len(NClients))
	i = uint64(0)
	for i < uint64(len(NClients)) {
		j := i
		go func(c *NClient, workload []uint64, writeServer uint64, readServer uint64) error {
			index := uint64(0)
			barrier.Done()
			barrier.Wait()
			defer wg.Done()

			start_time := time.Now()
			end_time := time.Now()
			latency := time.Duration(0)
			var serverId uint64

			for index < uint64(numberOfOperations) {
				if workload[index] == 0 {
					serverId = readServer
				} else {
					serverId = writeServer
				}
				// serverId = uint64(rand.Uint64() % uint64((len(servers))))

				if index == uint64(lower_bound) {
					start_time = time.Now()
				}
				if index == uint64(upper_bound) {
					end_time = time.Now()
				}
				// fmt.Println(index)
				v := uint64(rand.Int64())

				outGoingMessage := handler(c, workload[index], serverId, v, server.Message{})

				var m server.Message

				sent_time := time.Now()
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
				latency = latency + (time.Since(sent_time))

				handler(c, 2, 0, 0, m)
				index++
			}

			l.Lock()
			if !set {
			   avg_time = (end_time.Sub(start_time).Seconds())
			   set = true
			} else {
			  avg_time = (avg_time + (end_time.Sub(start_time).Seconds())) / 2
			}
			total_latency = total_latency + latency
			//fmt.Println(end_time.Sub(start_time).Seconds())
			l.Unlock()
			return nil
		}(NClients[j], workload[j], pinnedWriteServer[j], pinnedReadServer[j])

		i += 1
	}

	wg.Wait()

	fmt.Println("throughput:", int(float64(ops) / (avg_time)), "ops/sec")
	fmt.Println("latency:", int(float64(total_latency.Microseconds())/float64(ops)), "us")

	time.Sleep(100 * time.Millisecond)

	// put this in a wait group
	index := uint64(0)
	for index < uint64(len(servers)) {
		outGoingMessage := server.Message{MessageType: 4}
		err := NClients[0].ServerEncoder[index].Encode(&outGoingMessage)
		if err != nil {
			fmt.Print(err)
		}
		index++
	}

	// fmt.Println(ops / int(total_time))
	return nil
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
