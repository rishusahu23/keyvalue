package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"os"
)

// Command represents a set or delete operation
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// KeyValueStore represents a thread-safe key-value store
type KeyValueStore struct {
	mu    sync.RWMutex
	store map[string]string
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		store: make(map[string]string),
	}
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, exists := kv.store[key]
	return value, exists
}

func (kv *KeyValueStore) Put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store[key] = value
}

func (kv *KeyValueStore) Delete(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.store, key)
}

// FSM implements the raft.FSM interface
type FSM struct {
	kv *KeyValueStore
}

// NewFSM creates a new FSM instance
func NewFSM(kv *KeyValueStore) *FSM {
	return &FSM{
		kv: kv,
	}
}

// Apply applies a Raft log entry to the key-value store
func (f *FSM) Apply(log *raft.Log) interface{} {
	var command Command
	if err := json.Unmarshal(log.Data, &command); err != nil {
		panic(err)
	}
	switch command.Op {
	case "set":
		f.kv.Put(command.Key, command.Value)
	case "delete":
		f.kv.Delete(command.Key)
	}
	return nil
}

// Snapshot returns a snapshot of the current state of the FSM
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.kv.mu.RLock()
	defer f.kv.mu.RUnlock()
	storeCopy := make(map[string]string)
	for k, v := range f.kv.store {
		storeCopy[k] = v
	}
	return &Snapshot{store: storeCopy}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	var store map[string]string
	if err := json.NewDecoder(rc).Decode(&store); err != nil {
		return err
	}
	f.kv.mu.Lock()
	defer f.kv.mu.Unlock()
	f.kv.store = store
	return nil
}

// Snapshot represents a snapshot of the FSM
type Snapshot struct {
	store map[string]string
}

// Persist saves the snapshot to the given sink
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	if err := func() error {
		data, err := json.Marshal(s.store)
		if err != nil {
			return err
		}
		if _, err := sink.Write(data); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

// Release releases the snapshot
func (s *Snapshot) Release() {}

var (
	kvStore  *KeyValueStore
	raftNode *raft.Raft
)

func main() {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID("server1") // Set a unique LocalID for the node

	store, err := raftboltdb.NewBoltStore("raft.db")
	if err != nil {
		log.Fatal(err)
	}

	logStore, err := raftboltdb.NewBoltStore("raft-log.db")
	if err != nil {
		log.Fatal(err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(".", 1, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	address := ":8080"
	//advertiseAddress := "127.0.0.1:8080" // Replace with a proper address if needed

	transport, err := raft.NewTCPTransport(address, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080}, 3, 5*time.Second, os.Stderr)
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

	// Optional: Bootstrap or join a Raft cluster here if needed

	// Set up HTTP handlers for client interactions
	http.HandleFunc("/get", func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if value, exists := kvStore.Get(key); exists {
			fmt.Fprintf(w, "Value: %s\n", value)
		} else {
			http.Error(w, "Key not found", http.StatusNotFound)
		}
	})

	http.HandleFunc("/put", func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		value := req.URL.Query().Get("value")

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
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Fatal(http.ListenAndServe("0.0.0.0:"+port, nil))
}

// http://localhost:8081/put?key=mykey&value=myvalue
// http://localhost:8081/get?key=rishu
