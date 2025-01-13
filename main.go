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

	if len(os.Args) < 3 {
		log.Fatalf("usage: %s [client|server] [id]", os.Args[0])
	}

	id, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		log.Fatalf("can't convert %s to int: %s", os.Args[2], err)
	}

	switch os.Args[1] {
	case "client":
		client.New(id, servers).Start()
	case "server":
		server.New(id, servers[id], servers).Start()
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
