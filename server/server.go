package server

import (
	"encoding/gob"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/alanwang67/session_semantics/protocol"
)

type Operation struct {
	VersionVector []uint64
	Data          uint64
}

type Message struct {
	MessageType uint64

	C2S_Client_Id            uint64
	C2S_Server_Id            uint64
	C2S_Client_OperationType uint64
	C2S_Client_Data          uint64
	C2S_Client_VersionVector []uint64

	S2S_Gossip_Sending_ServerId   uint64
	S2S_Gossip_Receiving_ServerId uint64
	S2S_Gossip_Operations         []Operation
	S2S_Gossip_Index              uint64

	S2S_Acknowledge_Gossip_Sending_ServerId   uint64
	S2S_Acknowledge_Gossip_Receiving_ServerId uint64
	S2S_Acknowledge_Gossip_Index              uint64

	S2C_Client_OperationType uint64
	S2C_Client_Data          uint64
	S2C_Client_VersionVector []uint64
	S2C_Server_Id            uint64
	S2C_Client_Number        uint64
}

type NServer struct {
	Id                uint64
	Self              *protocol.Connection
	Peers             []*protocol.Connection
	PeerConnection    map[uint64]*gob.Encoder
	PeerAckConnection map[uint64]*gob.Encoder
	Clients           map[uint64]*gob.Encoder

	UnsatisfiedRequests    []Message
	VectorClock            []uint64
	OperationsPerformed    []Operation
	MyOperations           []Operation
	PendingOperations      []Operation
	GossipAcknowledgements []uint64
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
}

func New(id uint64, self *protocol.Connection, peers []*protocol.Connection) *NServer {
	server := &NServer{
		Id:                     id,
		Self:                   self,
		Peers:                  peers,
		PeerConnection:         make(map[uint64]*gob.Encoder),
		PeerAckConnection:      make(map[uint64]*gob.Encoder),
		Clients:                make(map[uint64]*gob.Encoder),
		UnsatisfiedRequests:    make([]Message, 0),
		VectorClock:            make([]uint64, len(peers)),
		OperationsPerformed:    make([]Operation, 0),
		MyOperations:           make([]Operation, 0),
		PendingOperations:      make([]Operation, 0),
		GossipAcknowledgements: make([]uint64, len(peers)),
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

func lexicographicCompare(v1 []uint64, v2 []uint64) bool {
	var output = false
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

func oneOffVersionVector(v1 []uint64, v2 []uint64) bool {
	var output = true
	var canApply = true
	var i = uint64(0)
	var l = uint64(len(v1))

	for i < l {
		if canApply && v1[i]+1 == v2[i] {
			canApply = false
			i = i + 1
			continue
		}
		if v1[i] < v2[i] {
			output = false
		}
		i = i + 1
	}

	return output && !canApply
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
	return equalSlices(o1.VersionVector, o2.VersionVector) && (o1.Data == o2.Data)
}

func binarySearch(s []Operation, needle Operation) uint64 {
	var i = uint64(0)
	var j = uint64(len(s))
	for i < j {
		mid := i + (j-i)/2
		if lexicographicCompare(needle.VersionVector, s[mid].VersionVector) {
			i = mid + 1
		} else {
			j = mid
		}
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
	if (len(l1) == 0) && (len(l2) == 0) {
		return make([]Operation, 0)
	}

	var output = append([]Operation{}, l1...)
	var i = uint64(0)
	var l = uint64(len(l2))

	for i < l {
		output = sortedInsert(output, l2[i])
		i++
	}

	var prev = uint64(1)
	var curr = uint64(1)
	for curr < uint64(len(output)) {
		if !equalOperations(output[curr-1], output[curr]) {
			output[prev] = output[curr]
			prev = prev + 1
		}
		curr = curr + 1
	}

	return output[:prev]
}

func deleteAtIndexOperation(l []Operation, index uint64) []Operation {
	var ret = make([]Operation, 0)
	ret = append(ret, l[:index]...)
	return append(ret, l[index+1:]...)
}

func deleteAtIndexMessage(l []Message, index uint64) []Message {
	var ret = make([]Message, 0)
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

	var i = uint64(0)

	for i < uint64(len(server.PendingOperations)) {
		if oneOffVersionVector(server.VectorClock, server.PendingOperations[i].VersionVector) {
			server.OperationsPerformed = mergeOperations(server.OperationsPerformed, []Operation{server.PendingOperations[i]})
			server.VectorClock = maxTS(server.VectorClock, server.PendingOperations[i].VersionVector)
			server.PendingOperations = deleteAtIndexOperation(server.PendingOperations, i)
			continue
		}
		i = i + 1
	}

	return server
}

func acknowledgeGossip(server Server, request Message) Server {
	if request.S2S_Acknowledge_Gossip_Sending_ServerId >= uint64(len(server.GossipAcknowledgements)) {
		return server
	}
	server.GossipAcknowledgements[request.S2S_Acknowledge_Gossip_Sending_ServerId] = maxTwoInts(server.GossipAcknowledgements[request.S2S_Acknowledge_Gossip_Sending_ServerId], request.S2S_Acknowledge_Gossip_Index)
	return server
}

func getGossipOperations(server Server, serverId uint64) []Operation {
	var ret = make([]Operation, 0)
	if serverId >= uint64(len(server.GossipAcknowledgements)) || (server.GossipAcknowledgements[serverId] >= uint64(len(server.MyOperations))) {
		return ret
	}

	return append(ret, server.MyOperations[server.GossipAcknowledgements[serverId]:]...)
}

func processClientRequest(server Server, request Message) (bool, Server, Message) {
	var reply = Message{}

	if !compareVersionVector(server.VectorClock, request.C2S_Client_VersionVector) {
		return false, server, reply
	}

	if request.C2S_Client_OperationType == 0 {
		reply.MessageType = 4
		reply.S2C_Client_OperationType = 0
		reply.S2C_Client_Data = getDataFromOperationLog(server.OperationsPerformed)
		reply.S2C_Client_VersionVector = server.VectorClock
		reply.S2C_Server_Id = server.Id
		reply.S2C_Client_Number = request.C2S_Client_Id

		return true, server, reply
	} else {
		server.VectorClock[server.Id] += 1

		server.OperationsPerformed = sortedInsert(server.OperationsPerformed, Operation{
			VersionVector: append([]uint64(nil), server.VectorClock...),
			Data:          request.C2S_Client_Data,
		})

		server.MyOperations = sortedInsert(server.MyOperations, Operation{
			VersionVector: append([]uint64(nil), server.VectorClock...),
			Data:          request.C2S_Client_Data,
		})

		reply.MessageType = 4
		reply.S2C_Client_OperationType = 1
		reply.S2C_Client_Data = 0
		reply.S2C_Client_VersionVector = append([]uint64(nil), server.VectorClock...)
		reply.S2C_Server_Id = server.Id
		reply.S2C_Client_Number = request.C2S_Client_Id

		return true, server, reply
	}
}

func processRequest(server Server, request Message) (Server, []Message) {
	var outGoingRequests = make([]Message, 0)
	var s = server
	if request.MessageType == 0 {
		var succeeded = false
		var reply = Message{}
		if len(request.C2S_Client_VersionVector) == 0 {
			fmt.Println(request)
			panic(request)
		}
		succeeded, s, reply = processClientRequest(s, request)
		if succeeded {
			outGoingRequests = append(outGoingRequests, reply)
		} else {
			s.UnsatisfiedRequests = append(s.UnsatisfiedRequests, request)
		}
	} else if request.MessageType == 1 {
		s = receiveGossip(s, request)
		outGoingRequests = append(outGoingRequests,
			Message{MessageType: 2,
				S2S_Acknowledge_Gossip_Sending_ServerId:   s.Id,
				S2S_Acknowledge_Gossip_Receiving_ServerId: request.S2S_Gossip_Sending_ServerId,
				S2S_Acknowledge_Gossip_Index:              request.S2S_Gossip_Index})

		var i = uint64(0)
		var reply = Message{}
		var succeeded = false

		for i < uint64(len(s.UnsatisfiedRequests)) {
			succeeded, s, reply = processClientRequest(s, s.UnsatisfiedRequests[i])
			if succeeded {
				outGoingRequests = append(outGoingRequests, reply)
				s.UnsatisfiedRequests = deleteAtIndexMessage(s.UnsatisfiedRequests, i)
				continue
			}
			i++
		}

	} else if request.MessageType == 2 {
		s = acknowledgeGossip(s, request)
	} else if request.MessageType == 3 {
		var i = uint64(0)
		for i < server.NumberOfServers {
			if uint64(i) != uint64(s.Id) {
				index := uint64(i)
				operations := getGossipOperations(s, index)
				if uint64(len(operations)) != uint64(0) {

					outGoingRequests = append(outGoingRequests,
						Message{MessageType: 1,
							S2S_Gossip_Sending_ServerId:   s.Id,
							S2S_Gossip_Receiving_ServerId: index,
							S2S_Gossip_Operations:         operations,
							S2S_Gossip_Index:              uint64(len(s.MyOperations) - 1),
						})
				}
			}
			i = i + 1
		}
	}

	return s, outGoingRequests
}

func handler(s *NServer, request *Message) error {
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
		}, *request)

	s.UnsatisfiedRequests = ns.UnsatisfiedRequests
	s.VectorClock = ns.VectorClock
	s.OperationsPerformed = ns.OperationsPerformed
	s.MyOperations = ns.MyOperations
	s.PendingOperations = ns.PendingOperations
	s.GossipAcknowledgements = ns.GossipAcknowledgements

	go func() {
		i := uint64(0)
		l := uint64(len(outGoingRequest))
		for i < l {
			index := i
			if outGoingRequest[index].MessageType == 1 {
				// c, _ := net.Dial(s.Peers[outGoingRequest[index].S2S_Gossip_Receiving_ServerId].Network, s.Peers[outGoingRequest[index].S2S_Gossip_Receiving_ServerId].Address)
				// enc := gob.NewEncoder(s.PeerConnection[outGoingRequest[index].S2S_Gossip_Receiving_ServerId])
				// enc.Encode(&outGoingRequest[index])
				// c.Close()
				// fmt.Println(outGoingRequest[index])
				s.PeerConnection[outGoingRequest[index].S2S_Gossip_Receiving_ServerId].Encode(&outGoingRequest[index])
				// fmt.Println(err)
			} else if outGoingRequest[index].MessageType == 2 {
				// enc := gob.NewEncoder(s.PeerAckConnection[outGoingRequest[index].S2S_Acknowledge_Gossip_Receiving_ServerId])
				// enc.Encode(&outGoingRequest[index])
				s.PeerAckConnection[outGoingRequest[index].S2S_Acknowledge_Gossip_Receiving_ServerId].Encode(&outGoingRequest[index])
				// c.Close()
			} else if outGoingRequest[index].MessageType == 4 {
				s.Clients[outGoingRequest[index].S2C_Client_Number].Encode(&outGoingRequest[index])
			}
			i++
		}
	}()
	return nil
}

func Start(s *NServer) error {
	l, err := net.Listen(s.Self.Network, s.Self.Address)

	if err != nil {
		fmt.Println(err)
		return nil
	}

	go func() {
		// will this work from just being in scope?
		i := uint64(0)
		for i < uint64(len(s.Peers)) {
			if i != s.Id {
				for {
					c, err := net.Dial(s.Peers[i].Network, s.Peers[i].Address)
					if err != nil {
						continue
					}
					s.mu.Lock()
					enc := gob.NewEncoder(c)
					s.PeerConnection[i] = enc
					s.mu.Unlock()

					break
				}
				fmt.Println("Connected with conn", i)
			}
			i++
		}
	}()

	go func() {
		// will this work from just being in scope?
		i := uint64(0)
		for i < uint64(len(s.Peers)) {
			if i != s.Id {
				for {
					c, err := net.Dial(s.Peers[i].Network, s.Peers[i].Address)
					if err != nil {
						continue
					}
					s.mu.Lock()
					enc := gob.NewEncoder(c)
					s.PeerAckConnection[i] = enc
					s.mu.Unlock()

					break
				}
				fmt.Println("Connected with ack", i)
			}
			i++
		}
	}()

	go func() error {
		for {
			ms := 800
			// rand.IntN(20) + 30

			time.Sleep(time.Duration(ms) * time.Microsecond)

			s.mu.Lock()

			if len(s.MyOperations) == 0 {
				s.mu.Unlock()
				continue
			}

			request := Message{MessageType: 3}

			handler(s, &request)

			s.mu.Unlock()
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return nil
		}

		// fmt.Println(conn.RemoteAddr())

		go func(s *NServer, c net.Conn) error { // make a for loop here, have it wait until the client request, block on recv?
			dec := gob.NewDecoder(c)

			for {
				// fmt.Println(c)
				// check if this is a server or not
				m := Message{}
				err := dec.Decode(&m)
				if err != nil {
					// EOF is coming from here for some reason?
					fmt.Print(err)
					return err
				}

				s.mu.Lock()

				fmt.Println("message: ", m)
				// fmt.Println("server: ", s, "\n")
				if m.MessageType == 0 {
					_, ok := s.Clients[m.C2S_Client_Id]
					if !ok {
						enc := gob.NewEncoder(c)
						s.Clients[m.C2S_Client_Id] = enc
					}
				}

				// for testing purposes
				if m.MessageType == 4 {
					fmt.Println(s.OperationsPerformed)
				}

				handler(s, &m)
				s.mu.Unlock()
			}
		}(s, conn)
	}
}

// use channels to order
// have clients thread
// have servers connect by loop

// what is EOF message,
