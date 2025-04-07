package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"runtime/pprof"
	"time"
	"fmt"
	// "runtime/debug"

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

	f, _ := os.Create("profiler" + os.Args[2] + os.Args[3])

	pprof.StartCPUProfile(f)

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
		// first argument is path to program

		fileLocation := os.Args[3]
		clientConfig, _ := os.ReadFile(fileLocation)

		var data map[string]interface{}
		json.Unmarshal(clientConfig, &data)

		threads, _ := strconv.ParseUint(os.Args[4], 10, 64)
		time, _ := strconv.ParseUint(os.Args[5], 10, 64)
		sessionSemantic, _ := strconv.ParseUint(os.Args[6], 10, 64)
		randomServer := data["randomServer"].(bool)
		switchServer := uint64(data["switchServer"].(float64))

		var l []interface{}
		l = data["writeServers"].([]interface{})
		writeServer := make([]uint64, len(l))
		for i, v := range l {
			writeServer[i] = uint64(v.(float64))
		}

		l = data["readServers"].([]interface{})
		readServer := make([]uint64, len(l))
		for i, v := range l {
			readServer[i] = uint64(v.(float64))
		}

		conf := client.ConfigurationInfo{
			Threads:         threads,
			Time:            time,
			SessionSemantic: sessionSemantic,
			RandomServer:    randomServer,
			WriteServer:     writeServer,
			ReadServer:      readServer,
			SwitchServer:    switchServer,
		}
		fmt.Println(conf)
		client.Start(conf, servers)
	case "server":
		if len(os.Args) < 4 {
			log.Fatalf("usage: go run main.go _ server [id] [gossip_interval]")
		}

		id, _ := strconv.ParseUint(os.Args[3], 10, 64)

		gossipInterval, _ := strconv.ParseUint(os.Args[4], 10, 64)

		go func() {
			server.Start(server.New(id, servers[id], servers, gossipInterval))
		}()

		time.Sleep(88 * time.Second)
		n := time.Now()

		for {
			if time.Since(n) > time.Duration(5*time.Second) {
				break
			}
		}
		pprof.StopCPUProfile()
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
