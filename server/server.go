package server

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Connection struct {
	Network string
	Address string
}

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
	Client_ReadVector    []uint64
	Client_WriteVector   []uint64

	Gossip_ServerId   uint64
	Gossip_Operations []Operation
}

type Reply struct {
	ReplyType uint64

	Client_Succeeded     bool
	Client_OperationType uint64
	Client_Data          uint64
	Client_ReadVector    []uint64
	Client_WriteVector   []uint64
}

type Server struct {
	Id    uint64
	Self  *Connection
	Peers []*Connection

	VectorClock         []uint64
	OperationsPerformed []Operation
	MyOperations        []Operation
	PendingOperations   []Operation
	Data                uint64
	mu                  sync.Mutex
}

func New(id uint64, self *Connection, peers []*Connection) *Server {
	server := &Server{
		Id:                  id,
		Self:                self,
		Peers:               peers,
		VectorClock:         make([]uint64, len(peers)),
		MyOperations:        make([]Operation, 0),
		OperationsPerformed: make([]Operation, 0),
		PendingOperations:   make([]Operation, 0),
		Data:                0,
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

func concurrentVersionVector(v1 []uint64, v2 []uint64) bool {
	return !compareVersionVector(v1, v2) && !compareVersionVector(v2, v1)
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

func compareOperations(o1 *Operation, o2 *Operation) bool {
	if concurrentVersionVector(o1.VersionVector, o2.VersionVector) {
		return lexiographicCompare(o1.VersionVector, o2.VersionVector)
	}
	return compareVersionVector(o1.VersionVector, o2.VersionVector)
}

func compareSlices(s1 []uint64, s2 []uint64) bool {
	var output = true
	var i = uint64(0)
	var l = uint64(len(s1))

	for i < l {
		if s1[i] != s2[i] {
			output = false
		}
		i++
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

func dependencyCheck(TS []uint64, request *Request) bool {
	if request.Client_SessionType == 0 { // Monotonic Reads
		return compareVersionVector(TS, request.Client_ReadVector)
	}
	if request.Client_SessionType == 1 { // Writes Follow Reads
		return compareVersionVector(TS, request.Client_ReadVector)
	}
	if request.Client_SessionType == 2 { // Read Your Writes
		return compareVersionVector(TS, request.Client_WriteVector)
	}
	if request.Client_SessionType == 3 { // Monotonics Writes
		return compareVersionVector(TS, request.Client_WriteVector)
	}
	if request.Client_SessionType == 4 { // Causal
		return (compareVersionVector(TS, request.Client_ReadVector) && compareVersionVector(TS, request.Client_WriteVector))
	}

	panic("Invalid Session Type")
}

func ProcessClientRequest(server *Server, request *Request) Reply { // do we want to pass in a pointer or the entire object
	reply := Reply{}

	if !(dependencyCheck(server.VectorClock, request)) {
		reply.Client_Succeeded = false
		return reply
	}

	if request.Client_OperationType == 0 {
		reply.Client_Succeeded = true
		reply.Client_OperationType = 0
		reply.Client_Data = server.Data
		reply.Client_ReadVector = maxTS(request.Client_ReadVector, server.VectorClock)
		reply.Client_WriteVector = request.Client_WriteVector

		return reply
	} else {
		server.VectorClock[server.Id] += 1
		server.Data = request.Client_Data

		op := Operation{
			OperationType: 1,
			VersionVector: append([]uint64(nil), server.VectorClock...),
			Data:          server.Data,
		}

		server.OperationsPerformed = append(server.OperationsPerformed, op)
		server.MyOperations = append(server.MyOperations, op) // we can reuse op because it should be immutable

		reply.Client_Succeeded = true
		reply.Client_OperationType = 1
		reply.Client_Data = server.Data
		reply.Client_ReadVector = request.Client_ReadVector
		reply.Client_WriteVector = append([]uint64(nil), server.VectorClock...)
		return reply
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

func equalOperations(o1 *Operation, o2 *Operation) bool {
	return (o1.OperationType == o2.OperationType) && compareSlices(o1.VersionVector, o2.VersionVector) && (o1.Data == o2.Data)
}

func binarySearch(s []Operation, needle *Operation) (uint64, bool) {
	var i = uint64(0)
	var j = uint64(len(s))
	for i < j {
		mid := i + (j-i)/2
		if compareOperations(needle, &s[mid]) {
			i = mid + 1
		} else {
			j = mid
		}
	}
	if i < uint64(len(s)) {
		return i, equalOperations(&s[i], needle)
	}
	return i, false
}

func sortedInsert(s []Operation, value Operation) []Operation {
	index, _ := binarySearch(s, &value)
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
		if !equalOperations(&output[curr-1], &output[curr]) {
			output[prev] = output[curr]
			prev++
		}
	}

	return output[:prev]
}

func deleteAtIndex(l []Operation, index uint64) []Operation {
	return append(l[:index], l[index+1:]...)
}

func receiveGossip(server *Server, request *Request) Reply {
	reply := Reply{}

	if len(request.Gossip_Operations) == 0 {
		return reply
	}

	server.PendingOperations = mergeOperations(server.PendingOperations, request.Gossip_Operations)
	latestVersionVector := append([]uint64(nil), server.VectorClock...)

	i := uint64(0)

	for i < uint64(len(server.PendingOperations)) { // we need to perform an optimization here so that we go through the entire list, maybe a delete at index function
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

	// server.PendingOperations = server.PendingOperations[i:]
	return reply
}

func getGossipOperations(server *Server) []Operation {
	return server.MyOperations
}

func (s *Server) ProcessRequest(request *Request, reply *Reply) error {
	s.mu.Lock()

	if request.RequestType == 0 {
		response := ProcessClientRequest(s, request)
		reply.Client_Succeeded = response.Client_Succeeded
		reply.Client_Data = response.Client_Data
		reply.Client_OperationType = response.Client_OperationType
		reply.Client_ReadVector = response.Client_ReadVector
		reply.Client_WriteVector = response.Client_WriteVector
	} else {
		receiveGossip(s, request)
	}

	// fmt.Println(s.OperationsPerformed, s.MyOperations, "\n")
	s.mu.Unlock()

	return nil
}

// for testing purposes
func (s *Server) PrintData(request *Request, reply *Reply) error {
	s.mu.Lock()
	fmt.Println(s.OperationsPerformed)
	// fmt.Println("Vector Clock", s.VectorClock)
	// fmt.Println("Operations Performed", s.OperationsPerformed)
	// fmt.Println("Pending Operations", s.PendingOperations)
	// fmt.Println("My Operations", s.MyOperations)
	s.mu.Unlock()
	return nil
}

func (s *Server) Start() error {

	l, _ := net.Listen(s.Self.Network, s.Self.Address)
	defer l.Close()

	rpc.Register(s)

	go func() {
		for {
			ms := 50
			time.Sleep(time.Duration(ms) * time.Millisecond)

			s.mu.Lock()

			if len(s.MyOperations) == 0 {
				s.mu.Unlock()
				continue
			}
			operations := append([]Operation{}, getGossipOperations(s)...)

			s.mu.Unlock()

			for i := range s.Peers {
				if uint64(i) != uint64(s.Id) {
					s.mu.Lock()
					request := Request{RequestType: 1, Gossip_ServerId: s.Id, Gossip_Operations: operations}
					reply := Reply{}
					s.mu.Unlock()

					h, _ := rpc.Dial(s.Peers[i].Network, s.Peers[i].Address)
					h.Call("Server.ProcessRequest", &request, &reply)
				}
			}
		}
	}()

	for {
		rpc.Accept(l)

	}
}