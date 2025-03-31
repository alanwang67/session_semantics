package main

import (
	"encoding/json"
	"log"
	"math/rand/v2"
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

		workload := make([][]uint64, threads)
		i := uint64(0)
		for i < uint64(len(workload)) {
			j := 0
			tmp := make([]uint64, numberOfOperations)
			for j < int(numberOfOperations) {
				tmp[j] = uint64(1)
				j += 1
			}
			workload[i] = tmp
			i += 1
		}

		sessionSemantics := make([]uint64, threads)
		writeServer := make([]uint64, threads)
		readServer := make([]uint64, threads)

		i = uint64(0)
		for i < uint64(threads) {
			sessionSemantics[i] = 5
			writeServer[i] = uint64(rand.Uint64() % uint64((len(servers))))
			readServer[i] = uint64(rand.Uint64() % uint64((len(servers))))
			i++
		}

		client.Start(threads, sessionSemantics, workload, writeServer, readServer, servers)
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
