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
		if len(os.Args) < 4 {
			log.Fatalf("usage: go run main.go client [threads] [operations]")
			return
		}

		threads, err := strconv.ParseUint(os.Args[2], 10, 64)
		if err != nil {
			log.Fatalf("can't convert %s to int: %s", os.Args[2], err)
		}

		numberOfOperations, err := strconv.ParseUint(os.Args[3], 10, 64)
		if err != nil {
			log.Fatalf("can't convert %s to int: %s", os.Args[3], err)
		}

		sessionSemantics := make([]uint64, threads)
		pinnedServer := make([]uint64, threads)

		i := uint64(0)
		for i < uint64(threads) {
			sessionSemantics[i] = 5
			pinnedServer[i] = 0
			i++
		}

		client.Start(threads, numberOfOperations, sessionSemantics, pinnedServer, servers)
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
