package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DBEntry struct {
	ValueJSON string `json:"value_json"`
	Timestamp int64  `json:"timestamp"`
}

type DBServer struct {
	pb.UnimplementedDBNodeServiceServer
	nodeID     string
	dbFilePath string
	brokerAddr string
	peers      []string
	mu         sync.RWMutex
	data       map[string]DBEntry
}

func NewDBServer(nodeID, dbFilePath, brokerAddr string, peers []string) *DBServer {
	return &DBServer{
		nodeID:     nodeID,
		dbFilePath: dbFilePath,
		brokerAddr: brokerAddr,
		peers:      peers,
		data:       make(map[string]DBEntry),
	}
}

// Load database from file
func (s *DBServer) loadDB() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.dbFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			s.data = make(map[string]DBEntry)
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&s.data); err != nil {
		return err
	}
	log.Printf("[%s] Loaded %d records from file %s\n", s.nodeID, len(s.data), s.dbFilePath)
	return nil
}

// Save database to file
func (s *DBServer) saveDB() error {
	// Call this while holding s.mu (or Lock beforehand)
	file, err := os.Create(s.dbFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(s.data)
}

func (s *DBServer) WriteData(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[%s] Write request: Key=%s, Timestamp=%d\n", s.nodeID, req.Key, req.Timestamp)

	existing, exists := s.data[req.Key]
	if exists && existing.Timestamp >= req.Timestamp {
		log.Printf("[%s] Write ignored due to LWW: existing timestamp %d >= request timestamp %d\n", s.nodeID, existing.Timestamp, req.Timestamp)
		return &pb.WriteResponse{
			Success: true,
			Message: "Write ignored (newer or same version exists)",
		}, nil
	}

	s.data[req.Key] = DBEntry{
		ValueJSON: req.ValueJson,
		Timestamp: req.Timestamp,
	}

	if err := s.saveDB(); err != nil {
		log.Printf("[%s] Error saving DB: %v\n", s.nodeID, err)
		return &pb.WriteResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to save data: %v", err),
		}, nil
	}

	return &pb.WriteResponse{
		Success: true,
		Message: "Write successful",
	}, nil
}

func (s *DBServer) ReadData(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Printf("[%s] Read request: Key=%s\n", s.nodeID, req.Key)

	// If client requests all events
	if req.Key == "all_events" {
		var events []string
		var maxTimestamp int64

		for k, entry := range s.data {
			if strings.HasPrefix(k, "event:") {
				events = append(events, entry.ValueJSON)
				if entry.Timestamp > maxTimestamp {
					maxTimestamp = entry.Timestamp
				}
			}
		}

		eventsJSON, err := json.Marshal(events)
		if err != nil {
			return &pb.ReadResponse{
				Success: false,
				Message: fmt.Sprintf("Error encoding events: %v", err),
			}, nil
		}

		return &pb.ReadResponse{
			Success:   true,
			ValueJson: string(eventsJSON),
			Timestamp: maxTimestamp,
		}, nil
	}

	entry, exists := s.data[req.Key]
	if !exists {
		return &pb.ReadResponse{
			Success: false,
			Message: "Key not found",
		}, nil
	}

	return &pb.ReadResponse{
		Success:   true,
		ValueJson: entry.ValueJSON,
		Timestamp: entry.Timestamp,
	}, nil
}

func (s *DBServer) SyncNode(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log.Printf("[%s] Sync request from %s\n", s.nodeID, req.RequesterNodeId)

	syncData := make(map[string]*pb.DBEntry)
	for k, v := range s.data {
		syncData[k] = &pb.DBEntry{
			ValueJson: v.ValueJSON,
			Timestamp: v.Timestamp,
		}
	}

	return &pb.SyncResponse{
		DbData: syncData,
	}, nil
}

func (s *DBServer) syncWithPeers() {
	log.Printf("[%s] Starting synchronization with peers...\n", s.nodeID)
	updated := false

	for _, peer := range s.peers {
		if peer == "" {
			continue
		}

		log.Printf("[%s] Connecting to peer %s...\n", s.nodeID, peer)
		conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Failed to connect to peer %s: %v\n", s.nodeID, peer, err)
			continue
		}

		client := pb.NewDBNodeServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.SyncNode(ctx, &pb.SyncRequest{RequesterNodeId: s.nodeID})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("[%s] Failed to sync with peer %s: %v\n", s.nodeID, peer, err)
			continue
		}

		s.mu.Lock()
		for k, peerEntry := range resp.DbData {
			existing, exists := s.data[k]
			if !exists || peerEntry.Timestamp > existing.Timestamp {
				s.data[k] = DBEntry{
					ValueJSON: peerEntry.ValueJson,
					Timestamp: peerEntry.Timestamp,
				}
				updated = true
				log.Printf("[%s] Synced key %s from peer (timestamp %d)\n", s.nodeID, k, peerEntry.Timestamp)
			}
		}
		s.mu.Unlock()
	}

	if updated {
		s.mu.Lock()
		s.saveDB()
		s.mu.Unlock()
		log.Printf("[%s] Sync complete. Local database updated.\n", s.nodeID)
	} else {
		log.Printf("[%s] Sync complete. No updates needed.\n", s.nodeID)
	}
}

func (s *DBServer) registerInBroker(listenAddr string) {
	for {
		log.Printf("[%s] Registering at Broker %s...\n", s.nodeID, s.brokerAddr)
		conn, err := grpc.Dial(s.brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Broker not available: %v. Retrying in 3s...\n", s.nodeID, err)
			time.Sleep(3 * time.Second)
			continue
		}

		client := pb.NewBrokerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.RegisterEntity(ctx, &pb.RegisterRequest{
			EntityId:   s.nodeID,
			EntityType: pb.EntityType_DBNODE,
			Address:    listenAddr,
		})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("[%s] Registration error: %v. Retrying in 3s...\n", s.nodeID, err)
			time.Sleep(3 * time.Second)
			continue
		}

		if !resp.Success {
			log.Printf("[%s] Registration rejected: %s. Retrying in 3s...\n", s.nodeID, resp.Message)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("[%s] Registered successfully in Broker!\n", s.nodeID)
		break
	}
}

func main() {
	nodeID := flag.String("id", "DB1", "Node identifier (DB1, DB2, DB3)")
	port := flag.String("port", "50052", "Listening port")
	brokerAddr := flag.String("broker", "localhost:50051", "Broker address")
	peersList := flag.String("peers", "", "Comma-separated list of peer addresses")
	dbFile := flag.String("dbfile", "", "DB persistence file path")
	flag.Parse()

	filePath := *dbFile
	if filePath == "" {
		filePath = fmt.Sprintf("db_data_%s.json", *nodeID)
	}

	var peers []string
	if *peersList != "" {
		peers = strings.Split(*peersList, ",")
	}

	log.Printf("[%s] Starting DB Node on port %s...\n", *nodeID, *port)

	server := NewDBServer(*nodeID, filePath, *brokerAddr, peers)
	if err := server.loadDB(); err != nil {
		log.Fatalf("Error loading DB file: %v", err)
	}

	// Run sync on startup (re-joining network)
	go func() {
		// Wait a bit for other nodes to start if booting together
		time.Sleep(2 * time.Second)
		server.syncWithPeers()
	}()

	// Register with Broker
	// If running inside docker, we can use the service name which is nodeID lowercased
	nodeIP := os.Getenv("NODE_IP")
	var containerAddr string
	if nodeIP != "" {
		containerAddr = fmt.Sprintf("%s:%s", nodeIP, *port)
	} else {
		containerAddr = fmt.Sprintf("%s:%s", strings.ToLower(*nodeID), *port)
	}

	go server.registerInBroker(containerAddr)

	// Start listening
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterDBNodeServiceServer(grpcServer, server)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
