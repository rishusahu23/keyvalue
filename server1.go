// server1.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/serialx/hashring"
)

func main() {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID("server1") // Set a unique LocalID for the node

	store, err := raftboltdb.NewBoltStore("raft1.db")
	if err != nil {
		log.Fatal(err)
	}

	logStore, err := raftboltdb.NewBoltStore("raft-log1.db")
	if err != nil {
		log.Fatal(err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(".", 1, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	address := ":49152"
	//advertiseAddress := "127.0.0.1:8080" // Replace with a proper address if needed

	transport, err := raft.NewTCPTransport(address, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 49152}, 3, 5*time.Second, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	kvStore = NewKeyValueStore()
	fsm := NewFSM(kvStore)

	raftNode, err = raft.NewRaft(config, fsm, store, logStore, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}
	bootstrapConfig := raft.Configuration{
		Servers: []raft.Server{
			{ID: "server1", Address: transport.LocalAddr()},
		},
	}

	raftNode.BootstrapCluster(bootstrapConfig)

	ring = hashring.New(nodes) // Initialize the consistent hashing ring

	// Optional: Bootstrap or join a Raft cluster here if needed

	// Set up HTTP handlers for client interactions
	http.HandleFunc("/get", func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if node, _ := ring.GetNode(key); node == "server1" {
			if value, exists := kvStore.Get(key); exists {
				fmt.Fprintf(w, "Value: %s\n", value)
			} else {
				http.Error(w, "Key not found", http.StatusNotFound)
			}
		} else {
			http.Error(w, "Key not handled by this node", http.StatusBadRequest)
		}
	})

	http.HandleFunc("/put", func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		value := req.URL.Query().Get("value")

		if node, _ := ring.GetNode(key); node == "server1" {
			command := Command{
				Op:    "set",
				Key:   key,
				Value: value,
			}
			data, err := json.Marshal(command)
			if err != nil {
				http.Error(w, "Error creating command", http.StatusInternalServerError)
				return
			}

			applyFuture := raftNode.Apply(data, 10*time.Second)
			if err := applyFuture.Error(); err != nil {
				http.Error(w, "Error applying command", http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "Stored\n")
		} else {
			http.Error(w, "Key not handled by this node", http.StatusBadRequest)
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Fatal(http.ListenAndServe("0.0.0.0:"+port, nil))
}
