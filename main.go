package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/alanwang67/session_semantics/client"
	"github.com/alanwang67/session_semantics/protocol"
	"github.com/alanwang67/session_semantics/server"
)

func main() {
	config, _ := os.ReadFile("config.json")

	var data map[string]interface{}
	json.Unmarshal(config, &data)

	servers := make([]*protocol.Connection, len(data["servers"].([]interface{})))
	for i, s := range data["servers"].([]interface{}) {
		conn, _ := s.(map[string]interface{})

		network, _ := conn["network"].(string)
		address, _ := conn["address"].(string)

		servers[i] = &protocol.Connection{
			Network: network,
			Address: address,
		}
	}

	switch os.Args[1] {
	case "client":
		// first arugment is path to program
		if len(os.Args) < 5 {
			log.Fatalf("usage: go run main.go client [threads] [numberOfOperations] [sessionSemantic] [randomServer] [writeServers] [readServers]")
			return
		}

		threads, _ := strconv.ParseUint(os.Args[2], 10, 64)
		numberOfOperations, _ := strconv.ParseUint(os.Args[3], 10, 64)
		sessionSemantic, _ := strconv.ParseUint(os.Args[4], 10, 64)
		randomServer, _ := strconv.ParseBool(os.Args[5])

		workload := make([]uint64, numberOfOperations)
		i := uint64(0)
		for i < uint64(len(workload)) {
			workload[i] = uint64(1)
			i += 1
		}

		writeServer := make([]uint64, threads)
		readServer := make([]uint64, threads)

		i = uint64(0)
		for i < uint64(threads) {
			writeServer[i] = 0
			readServer[i] = 0
			i++
		}

		conf := client.ConfigurationInfo{
			Threads:            threads,
			NumberOfOperations: numberOfOperations,
			SessionSemantic:    sessionSemantic,
			Workload:           workload,
			RandomServer:       randomServer,
			WriteServer:        writeServer,
			ReadServer:         readServer,
		}

		client.Start(conf, servers)
	case "server":
		if len(os.Args) < 4 {
			log.Fatalf("usage: go run main.go server [id] [gossip_interval]")
		}

		id, err := strconv.ParseUint(os.Args[2], 10, 64)

		if err != nil {
			log.Fatalf("can't convert %s to int: %s", os.Args[2], err)
		}

		gossipInterval, err := strconv.ParseUint(os.Args[3], 10, 64)

		if err != nil {
			log.Fatalf("can't convert %s to int: %s", os.Args[3], err)
		}

		server.Start(server.New(id, servers[id], servers, gossipInterval))
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
