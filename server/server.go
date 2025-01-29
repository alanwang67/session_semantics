package server

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/alanwang67/session_semantics/protocol"
)

type Operation struct {
	OperationType uint64
	VersionVector []uint64
	Data          uint64
}

type Message struct {
	MessageType uint64

	C2S_Client_Id            uint64
	C2S_Client_RequestNumber uint64
	C2S_Client_OperationType uint64
	C2S_Client_Data          uint64
	C2S_Client_VersionVector []uint64

	S2S_Gossip_Sending_ServerId   uint64
	S2S_Gossip_Receiving_ServerId uint64
	S2S_Gossip_Operations         []Operation

	S2S_Acknowledge_Gossip_Sending_ServerId   uint64
	S2S_Acknowledge_Gossip_Receiving_ServerId uint64
	S2S_Acknowledge_Gossip_VersionVector      []uint64

	S2C_Client_OperationType uint64
	S2C_Client_Succeeded     bool
	S2C_Client_Data          uint64
	S2C_Client_VersionVector []uint64
	S2C_Client_RequestNumber uint64
	S2C_Client_Number        uint64
}

type RpcServer struct {
	Id      uint64
	Self    *protocol.Connection
	Peers   []*protocol.Connection
	Clients []*protocol.Connection

	UnsatisfiedRequests    []Message
	VectorClock            []uint64
	OperationsPerformed    []Operation
	MyOperations           []Operation
	PendingOperations      []Operation
	GossipAcknowledgements []uint64
	SeenRequests           []uint64
	mu                     sync.Mutex
}

type Server struct {
	Id                     uint64
	NumberOfServers        uint64
	UnsatisfiedRequests    []Message
	VectorClock            []uint64
	OperationsPerformed    []Operation
	MyOperations           []Operation
	PendingOperations      []Operation
	GossipAcknowledgements []uint64
	SeenRequests           []uint64
}

func New(id uint64, self *protocol.Connection, peers []*protocol.Connection, clients []*protocol.Connection) *RpcServer {
	server := &RpcServer{
		Id:                     id,
		Self:                   self,
		Peers:                  peers,
		Clients:                clients,
		UnsatisfiedRequests:    make([]Message, 0),
		VectorClock:            make([]uint64, len(peers)),
		OperationsPerformed:    make([]Operation, 0),
		MyOperations:           make([]Operation, 0),
		PendingOperations:      make([]Operation, 0),
		GossipAcknowledgements: make([]uint64, len(peers)),
		SeenRequests:           make([]uint64, len(clients)),
	}

	return server
}

func compareVersionVector(v1 []uint64, v2 []uint64) bool {
	var output = true
	var i = uint64(0)
	var l = uint64(len(v1))
	for i < l {
		if v1[i] < v2[i] {
			output = false
			break
		}
		i++
	}
	return output
}

func lexiographicCompare(v1 []uint64, v2 []uint64) bool {
	var output = true
	var i = uint64(0)
	var l = uint64(len(v1))
	for i < l {
		if v1[i] == v2[i] {
			i++
		} else {
			output = v1[i] > v2[i]
			break
		}
	}

	return output
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

func oneOffVersionVector(serverId uint64, v1 []uint64, v2 []uint64) bool {
	var output = true
	var canApply = true
	var i = uint64(0)
	var l = uint64(len(v1))

	for i < l {
		if i == uint64(serverId) {
			i++
		} else if canApply && v1[i]+1 == v2[i] {
			canApply = false
			i++
		} else if v1[i] < v2[i] {
			output = false
			i++
		} else {
			i++
		}
	}

	return output
}

func equalSlices(s1 []uint64, s2 []uint64) bool {
	var output = true
	var i = uint64(0)
	var l = uint64(len(s1))

	for i < l {
		if s1[i] != s2[i] {
			output = false
			break
		}
		i++
	}

	return output
}

func equalOperations(o1 Operation, o2 Operation) bool {
	return (o1.OperationType == o2.OperationType) && equalSlices(o1.VersionVector, o2.VersionVector) && (o1.Data == o2.Data)
}

func binarySearch(s []Operation, needle Operation) uint64 {
	var i = uint64(0)
	var j = uint64(len(s))
	for i < j {
		mid := i + (j-i)/2
		if lexiographicCompare(needle.VersionVector, s[mid].VersionVector) {
			i = mid + 1
		} else {
			j = mid
		}
	}
	if i < uint64(len(s)) {
		return i
	}
	return i
}

func sortedInsert(s []Operation, value Operation) []Operation {
	index := binarySearch(s, value)
	if uint64(len(s)) == index {
		return append(s, value)
	} else {
		right := append([]Operation{value}, s[index:]...)
		result := append(s[:index], right...)
		return result
	}
}

func mergeOperations(l1 []Operation, l2 []Operation) []Operation {
	var output = append([]Operation{}, l1...)
	var i = uint64(0)
	var l = uint64(len(l2))

	for i < l {
		output = sortedInsert(output, l2[i])
		i++
	}

	prev := 1
	for curr := 1; curr < len(output); curr++ {
		if !equalOperations(output[curr-1], output[curr]) {
			output[prev] = output[curr]
			prev++
		}
	}

	return output[:prev]
}

func deleteAtIndexOperation(l []Operation, index uint64) []Operation {
	var ret = make([]Operation, 0)
	ret = append(ret, l[:index]...)
	return append(ret, l[index+1:]...)
}

func getDataFromOperationLog(l []Operation) uint64 {
	if len(l) > 0 {
		return l[len(l)-1].Data
	}
	return 0
}

func receiveGossip(server Server, request Message) Server {
	if len(request.S2S_Gossip_Operations) == 0 {
		return server
	}

	server.PendingOperations = mergeOperations(server.PendingOperations, request.S2S_Gossip_Operations)
	latestVersionVector := append([]uint64(nil), server.VectorClock...)

	i := uint64(0)

	for i < uint64(len(server.PendingOperations)) {
		if oneOffVersionVector(server.Id, latestVersionVector, server.PendingOperations[i].VersionVector) {
			server.OperationsPerformed = mergeOperations(server.OperationsPerformed, []Operation{server.PendingOperations[i]})
			server.VectorClock = maxTS(latestVersionVector, server.PendingOperations[i].VersionVector)
			server.PendingOperations = deleteAtIndexOperation(server.PendingOperations, i)
			latestVersionVector = append([]uint64(nil), server.VectorClock...)
			continue
		}
		i++
	}

	return server
}

func acknowledgeGossip(server Server, request Message) Server {
	server.GossipAcknowledgements[request.S2S_Acknowledge_Gossip_Sending_ServerId] = request.S2S_Acknowledge_Gossip_VersionVector[server.Id]
	return server
}

func getGossipOperations(server Server, serverId uint64) []Operation {
	return append([]Operation(nil), server.MyOperations[server.GossipAcknowledgements[serverId]:]...)
}

func checkIfDuplicateRequest(server Server, request Message) bool {
	return server.SeenRequests[request.C2S_Client_Id] >= request.C2S_Client_RequestNumber
}

func deleteAtIndexMessage(l []Message, index uint64) []Message {
	var ret = make([]Message, 0)
	ret = append(ret, l[:index]...)
	return append(ret, l[index+1:]...)
}

func processClientRequest(server Server, request Message) (Server, Message) {
	reply := Message{}

	// fmt.Print(checkIfDuplicateRequest(server, request))
	if !(compareVersionVector(server.VectorClock, request.C2S_Client_VersionVector)) || checkIfDuplicateRequest(server, request) {
		reply.S2C_Client_Succeeded = false
		return server, reply
	}

	if request.C2S_Client_OperationType == 0 {
		server.SeenRequests[request.C2S_Client_Id] = request.C2S_Client_RequestNumber

		reply.MessageType = 4
		reply.S2C_Client_OperationType = 0
		reply.S2C_Client_Succeeded = true
		reply.S2C_Client_Data = getDataFromOperationLog(server.OperationsPerformed)
		reply.S2C_Client_VersionVector = maxTS(request.C2S_Client_VersionVector, server.VectorClock)
		reply.S2C_Client_Number = request.C2S_Client_Id
		reply.S2C_Client_RequestNumber = request.C2S_Client_RequestNumber

		return server, reply
	} else {
		server.VectorClock[server.Id] += 1
		server.SeenRequests[request.C2S_Client_Id] = request.C2S_Client_RequestNumber

		server.OperationsPerformed = append(server.OperationsPerformed, Operation{
			OperationType: 1,
			VersionVector: append([]uint64(nil), server.VectorClock...),
			Data:          request.C2S_Client_Data,
		})

		server.MyOperations = append(server.MyOperations, Operation{
			OperationType: 1,
			VersionVector: append([]uint64(nil), server.VectorClock...),
			Data:          request.C2S_Client_Data,
		})

		reply.MessageType = 4
		reply.S2C_Client_OperationType = 1
		reply.S2C_Client_Succeeded = true
		reply.S2C_Client_Data = getDataFromOperationLog(server.OperationsPerformed)
		reply.S2C_Client_VersionVector = append([]uint64(nil), server.VectorClock...)
		reply.S2C_Client_Number = request.C2S_Client_Id
		reply.S2C_Client_RequestNumber = request.C2S_Client_RequestNumber

		// fmt.Println(reply)

		return server, reply
	}
}

func processRequest(server Server, request Message) (Server, []Message) {
	outGoingRequests := make([]Message, 0)
	if request.MessageType == 0 { // Regular client request
		server, reply := processClientRequest(server, request)
		if !reply.S2C_Client_Succeeded {
			server.UnsatisfiedRequests = append(server.UnsatisfiedRequests, request)

			return server, outGoingRequests
		} else {
			outGoingRequests = append(outGoingRequests, reply)

			return server, outGoingRequests
		}
	} else if request.MessageType == 1 { // receiving a gossip request
		// fmt.Print(request.S2S_Gossip_Operations)
		server := receiveGossip(server, request)
		outGoingRequests = append(outGoingRequests,
			Message{MessageType: 2,
				S2S_Acknowledge_Gossip_Sending_ServerId:   server.Id,
				S2S_Acknowledge_Gossip_Receiving_ServerId: request.S2S_Gossip_Sending_ServerId,
				S2S_Acknowledge_Gossip_VersionVector:      request.S2S_Gossip_Operations[len(request.S2S_Gossip_Operations)-1].VersionVector})

		var i = uint64(0)
		var reply = Message{}

		for i < uint64(len(server.UnsatisfiedRequests)) {
			server, reply = processClientRequest(server, server.UnsatisfiedRequests[i])
			if reply.S2C_Client_Succeeded {
				outGoingRequests = append(outGoingRequests, reply)
				server.UnsatisfiedRequests = deleteAtIndexMessage(server.UnsatisfiedRequests, i)
				continue
			}
			i++
		}

		return server, outGoingRequests
	} else if request.MessageType == 2 { // acknowledging a gossip request
		server = acknowledgeGossip(server, request)
		return server, outGoingRequests
	} else if request.MessageType == 3 { // sending gossip request
		for i := range server.NumberOfServers {
			if uint64(i) != uint64(server.Id) {
				index := uint64(i)
				outGoingRequests = append(outGoingRequests,
					Message{MessageType: 1,
						S2S_Gossip_Sending_ServerId:   server.Id,
						S2S_Gossip_Receiving_ServerId: index,
						S2S_Gossip_Operations:         getGossipOperations(server, index),
					})
			}
		}
		return server, outGoingRequests
	}

	panic("Unknown MessageType")
}

func (s *RpcServer) RpcHandler(request *Message, reply *Message) error {
	s.mu.Lock()
	// fmt.Print(s.VectorClock)
	ns, outGoingRequest := processRequest(
		Server{
			Id:                     s.Id,
			NumberOfServers:        uint64(len(s.Peers)),
			UnsatisfiedRequests:    s.UnsatisfiedRequests,
			VectorClock:            s.VectorClock,
			OperationsPerformed:    s.OperationsPerformed,
			MyOperations:           s.MyOperations,
			PendingOperations:      s.PendingOperations,
			GossipAcknowledgements: s.GossipAcknowledgements,
			SeenRequests:           s.SeenRequests,
		}, *request)

	s.UnsatisfiedRequests = ns.UnsatisfiedRequests
	s.VectorClock = ns.VectorClock
	s.OperationsPerformed = ns.OperationsPerformed
	s.MyOperations = ns.MyOperations
	s.PendingOperations = ns.PendingOperations
	s.GossipAcknowledgements = ns.GossipAcknowledgements
	s.SeenRequests = ns.SeenRequests

	s.mu.Unlock()

	// send RPC request to client here
	// change request such that it has an address
	go func(peers []*protocol.Connection, clients []*protocol.Connection, outGoingRequest []Message) error {
		i := uint64(0)
		l := uint64(len(outGoingRequest))
		for i < l {
			index := i
			if outGoingRequest[index].MessageType == 1 {
				go func() {
					protocol.Invoke(*peers[outGoingRequest[index].S2S_Gossip_Receiving_ServerId], "RpcServer.RpcHandler", &outGoingRequest[index], &Message{})
				}()
			} else if outGoingRequest[index].MessageType == 4 {
				go func() {
					// fmt.Print("We are here")
					// fmt.Print(*clients[outGoingRequest[index].S2C_Client_Number])
					protocol.Invoke(*clients[outGoingRequest[index].S2C_Client_Number], "Client.AcknowledgeRequest", &outGoingRequest[index], &Message{})
				}()
			}
			i++
		}
		return nil
	}(s.Peers, s.Clients, outGoingRequest)

	return nil
}

// for testing purposes
func (s *RpcServer) PrintData(request *Message, reply *Message) error {
	s.mu.Lock()
	fmt.Println(s.OperationsPerformed)
	// fmt.Println("MyOperations", s.MyOperations)
	// fmt.Println("Vector Clock", s.VectorClock)
	// fmt.Println("Operations Performed", s.OperationsPerformed)
	// fmt.Println("Pending Operations", s.PendingOperations)
	// fmt.Println("My Operations", s.MyOperations)
	s.mu.Unlock()
	return nil
}

func (s *RpcServer) Start() error {

	l, _ := net.Listen(s.Self.Network, s.Self.Address)
	defer l.Close()

	rpc.Register(s)

	go func() error {
		for {
			ms := 50

			time.Sleep(time.Duration(ms) * time.Millisecond)

			s.mu.Lock()

			if len(s.MyOperations) == 0 {
				s.mu.Unlock()
				continue
			}

			id := s.Id
			request := Message{MessageType: 3}
			reply := Message{}

			s.mu.Unlock()

			protocol.Invoke(*s.Peers[id], "RpcServer.RpcHandler", &request, &reply)
		}
	}()

	for {
		rpc.Accept(l)
	}
}
