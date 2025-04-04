package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/alanwang67/session_semantics/client"
	"github.com/alanwang67/session_semantics/protocol"
	"github.com/alanwang67/session_semantics/server"
)

func processAddressString(address string, n uint64) string {
	l := strings.Split(address, `:`)
	i, _ := strconv.ParseUint(l[1], 10, 64)
	return l[0] + ":" + (strconv.Itoa(int(i + n)))
}

func main() {
	config, _ := os.ReadFile("config.json")

	portOffSet, _ := strconv.ParseUint(os.Args[1], 10, 64)

	var data map[string]interface{}
	json.Unmarshal(config, &data)

	servers := make([]*protocol.Connection, len(data["servers"].([]interface{})))
	for i, s := range data["servers"].([]interface{}) {
		conn, _ := s.(map[string]interface{})

		network, _ := conn["network"].(string)
		address, _ := conn["address"].(string)

		servers[i] = &protocol.Connection{
			Network: network,
			Address: processAddressString(address, portOffSet),
		}
	}

	switch os.Args[2] {
	case "client":
		// first arugment is path to program
		if len(os.Args) < 8 {
			log.Fatalf("usage: go run main.go _ client [threads] [numberOfOperations] [sessionSemantic] [randomServer] [writeServers] [readServers]")
			return
		}

		threads, _ := strconv.ParseUint(os.Args[3], 10, 64)
		time, _ := strconv.ParseUint(os.Args[4], 10, 64)
		sessionSemantic, _ := strconv.ParseUint(os.Args[5], 10, 64)
		randomServer, _ := strconv.ParseBool(os.Args[6])

		writeServer := make([]uint64, threads)
		readServer := make([]uint64, threads)

		i := uint64(0)
		for i < uint64(threads) {
			writeServer[i] = 0
			readServer[i] = 0
			i++
		}

		conf := client.ConfigurationInfo{
			Threads:         threads,
			Time:            time,
			SessionSemantic: sessionSemantic,
			RandomServer:    randomServer,
			WriteServer:     writeServer,
			ReadServer:      readServer,
		}

		client.Start(conf, servers)
	case "server":
		if len(os.Args) < 4 {
			log.Fatalf("usage: go run main.go _ server [id] [gossip_interval]")
		}

		id, _ := strconv.ParseUint(os.Args[3], 10, 64)

		gossipInterval, _ := strconv.ParseUint(os.Args[4], 10, 64)

		server.Start(server.New(id, servers[id], servers, gossipInterval))
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
