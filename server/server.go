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

type Request struct {
	RequestType uint64

	Client_OperationType uint64
	Client_SessionType   uint64
	Client_Data          uint64
	Client_Vector        []uint64

	Receive_Gossip_ServerId   uint64
	Receive_Gossip_Operations []Operation

	Acknowledge_Gossip_ServerId uint64
	Acknowledge_Gossip_Index    uint64

	Receiver_ServerId uint64
}

type Reply struct {
	Client_Succeeded     bool
	Client_OperationType uint64
	Client_Data          uint64
	Client_Vector        []uint64
}

type RpcServer struct {
	Id    uint64
	Self  *protocol.Connection
	Peers []*protocol.Connection

	VectorClock            []uint64
	OperationsPerformed    []Operation
	MyOperations           []Operation
	PendingOperations      []Operation
	Data                   uint64
	GossipAcknowledgements []uint64
	mu                     sync.Mutex
}

// maybe add is backup to this field?
type Server struct {
	Id                     uint64
	NumberOfServers        uint64
	VectorClock            []uint64
	OperationsPerformed    []Operation
	MyOperations           []Operation
	PendingOperations      []Operation
	Data                   uint64
	GossipAcknowledgements []uint64
}

func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *RpcServer {
	server := &RpcServer{
		Id:                     id,
		Self:                   self,
		Peers:                  peers,
		VectorClock:            make([]uint64, len(peers)),
		MyOperations:           make([]Operation, 0),
		OperationsPerformed:    make([]Operation, 0),
		PendingOperations:      make([]Operation, 0),
		GossipAcknowledgements: make([]uint64, len(peers)),
		Data:                   0,
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

func dependencyCheck(TS []uint64, request Request) bool {
	return compareVersionVector(TS, request.Client_Vector)
}

func processClientRequest(server Server, request Request) (Server, Reply) {
	reply := Reply{}

	if !(dependencyCheck(server.VectorClock, request)) {
		reply.Client_Succeeded = false
		return server, reply
	}

	if request.Client_OperationType == 0 {
		reply.Client_Succeeded = true
		reply.Client_OperationType = 0
		reply.Client_Data = server.Data
		reply.Client_Vector = maxTS(request.Client_Vector, server.VectorClock)

		return server, reply
	} else {
		server.VectorClock[server.Id] += 1
		server.Data = request.Client_Data

		op := Operation{
			OperationType: 1,
			VersionVector: append([]uint64(nil), server.VectorClock...),
			Data:          server.Data,
		}

		server.OperationsPerformed = append(server.OperationsPerformed, op) // would we need to do anything here since we are mutating the underlying slice?
		server.MyOperations = append(server.MyOperations, op)               // we can reuse op because it should be immutable

		reply.Client_Succeeded = true
		reply.Client_OperationType = 1
		reply.Client_Data = server.Data
		reply.Client_Vector = append([]uint64(nil), server.VectorClock...)
		return server, reply
	}
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
		// we need to do it like this since it could be dangerous if we reuse the slice again
		// we can verify the other one likely by using an invariant that we won't reuse
		// this would be O(n)
		right := append([]Operation{value}, s[index:]...) // https://stackoverflow.com/questions/37334119/how-to-delete-an-element-from-a-slice-in-golang
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

// https://stackoverflow.com/questions/16248241/concatenate-two-slices-in-go
func deleteAtIndex(l []Operation, index uint64) []Operation {
	var ret = make([]Operation, 0)
	ret = append(ret, l[:index]...)
	return append(ret, l[index+1:]...)
}

func receiveGossip(server Server, request Request) (Server, Reply) {
	reply := Reply{}

	// I don't think this will happen
	if len(request.Receive_Gossip_Operations) == 0 {
		return server, reply
	}

	server.PendingOperations = mergeOperations(server.PendingOperations, request.Receive_Gossip_Operations)
	latestVersionVector := append([]uint64(nil), server.VectorClock...)

	i := uint64(0)

	for i < uint64(len(server.PendingOperations)) {
		if oneOffVersionVector(server.Id, latestVersionVector, server.PendingOperations[i].VersionVector) {
			server.OperationsPerformed = mergeOperations(server.OperationsPerformed, []Operation{server.PendingOperations[i]})
			server.VectorClock = maxTS(latestVersionVector, server.PendingOperations[i].VersionVector)
			server.Data = server.OperationsPerformed[len(server.OperationsPerformed)-1].Data
			server.PendingOperations = deleteAtIndex(server.PendingOperations, i)
			latestVersionVector = append([]uint64(nil), server.VectorClock...)
			continue
		}
		i++
	}

	return server, reply
}

func acknowledgeGossip(server Server, request Request) Server {
	server.GossipAcknowledgements[request.Acknowledge_Gossip_ServerId] = request.Acknowledge_Gossip_Index
	return server
}

func getGossipOperations(server Server, serverId uint64) []Operation {
	return append([]Operation(nil), server.MyOperations[server.GossipAcknowledgements[serverId]:]...) // how can we eliminate this, problem of aliasing?
}

func processRequest(server Server, request Request) (Server, Reply, []Request) {
	if request.RequestType == 0 { // Regular client request
		server, outGoingreply := processClientRequest(server, request)
		outGoingRequests := make([]Request, 0)
		return server, outGoingreply, outGoingRequests
	} else if request.RequestType == 1 { // receiving a gossip request
		server, outGoingreply := receiveGossip(server, request)
		outGoingRequests := make([]Request, 0)
		outGoingRequests = append(outGoingRequests, Request{RequestType: 2, Acknowledge_Gossip_ServerId: server.Id, Receiver_ServerId: request.Receive_Gossip_ServerId, Acknowledge_Gossip_Index: uint64(len(request.Receive_Gossip_Operations))})
		return server, outGoingreply, outGoingRequests
	} else if request.RequestType == 2 { // acknowledging a gossip request
		server = acknowledgeGossip(server, request)
		outGoingRequests := make([]Request, 0)
		return server, Reply{}, outGoingRequests
	} else if request.RequestType == 3 { // sending gossip request
		outGoingRequests := make([]Request, 0)
		for i := range server.NumberOfServers {
			if uint64(i) != uint64(server.Id) {
				index := uint64(i)
				outGoingRequests = append(outGoingRequests, Request{RequestType: 1, Receive_Gossip_ServerId: server.Id, Receiver_ServerId: index, Receive_Gossip_Operations: getGossipOperations(server, index)})
			}
		}
		return server, Reply{}, outGoingRequests
	}

	panic("Not a valid request type")
}

func (s *RpcServer) RpcHandler(request *Request, reply *Reply) error {
	s.mu.Lock()

	ns, outGoingReply, outGoingRequest := processRequest(
		Server{Id: s.Id,
			NumberOfServers:        uint64(len(s.Peers)),
			VectorClock:            s.VectorClock,
			OperationsPerformed:    s.OperationsPerformed,
			MyOperations:           s.MyOperations,
			PendingOperations:      s.PendingOperations,
			Data:                   s.Data,
			GossipAcknowledgements: s.GossipAcknowledgements}, *request)

	s.VectorClock = ns.VectorClock
	s.OperationsPerformed = ns.OperationsPerformed
	s.MyOperations = ns.MyOperations
	s.PendingOperations = ns.PendingOperations
	s.Data = ns.Data
	s.GossipAcknowledgements = ns.GossipAcknowledgements

	reply.Client_Succeeded = outGoingReply.Client_Succeeded
	reply.Client_Data = outGoingReply.Client_Data
	reply.Client_OperationType = outGoingReply.Client_OperationType
	reply.Client_Vector = outGoingReply.Client_Vector

	s.mu.Unlock()

	go func(peers []*protocol.Connection, outGoingReply Reply, outGoingRequest []Request) error {
		i := uint64(0)
		l := uint64(len(outGoingRequest))
		for i < l {
			protocol.Invoke(*peers[outGoingRequest[i].Receiver_ServerId], "RpcServer.RpcHandler", &outGoingRequest[i], &Reply{})
			i++
		}
		return nil
	}(s.Peers, outGoingReply, outGoingRequest)

	return nil
}

// for testing purposes
// How do lock's in structs work in go
func (s *RpcServer) PrintData(request *Request, reply *Reply) error {
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
			ms := 50 // performs better with a higher ms for gossip, at first I was trying it with a lower time since it said can't server your request
			// but it's likely because it was spending a lot of time processing it without being able to apply everything
			time.Sleep(time.Duration(ms) * time.Millisecond)

			s.mu.Lock()

			if len(s.MyOperations) == 0 {
				s.mu.Unlock()
				continue
			}

			id := s.Id
			request := Request{RequestType: 3}
			reply := Reply{}

			s.mu.Unlock()
			protocol.Invoke(*s.Peers[id], "RpcServer.RpcHandler", &request, &reply)
		}
	}()

	for {
		rpc.Accept(l)
	}
}
