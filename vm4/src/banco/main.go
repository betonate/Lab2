package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BancoServer struct {
	pb.UnimplementedBancoServiceServer
	bankID     string
	brokerAddr string
	randSource *rand.Rand
}

func NewBancoServer(bankID, brokerAddr string) *BancoServer {
	return &BancoServer{
		bankID:     bankID,
		brokerAddr: brokerAddr,
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *BancoServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
	log.Printf("[%s] Received payment request from client %s, amount: %d, method: %s\n", s.bankID, req.ClientId, req.Amount, req.Method)

	// Determine probability
	threshold := 0.8 // 80% approval by default
	if strings.ToLower(req.Method) == "credito" {
		threshold = 0.9 // 90% approval for credit cards
	}

	val := s.randSource.Float64()
	if val < threshold {
		log.Printf("[%s] TRANSACTION APPROVED: client=%s, method=%s, amount=%d\n", s.bankID, req.ClientId, req.Method, req.Amount)
		return &pb.PaymentResponse{
			Approved: true,
			Message:  "Payment approved by USM Bank",
		}, nil
	}

	log.Printf("[%s] TRANSACTION REJECTED: client=%s, method=%s, amount=%d. Reason: Fondos insuficientes\n", s.bankID, req.ClientId, req.Method, req.Amount)
	return &pb.PaymentResponse{
		Approved: false,
		Message:  "Fondos insuficientes",
	}, nil
}

func (s *BancoServer) registerInBroker(listenAddr string) {
	for {
		log.Printf("[%s] Registering at Broker %s...\n", s.bankID, s.brokerAddr)
		conn, err := grpc.Dial(s.brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Broker not available: %v. Retrying in 3s...\n", s.bankID, err)
			time.Sleep(3 * time.Second)
			continue
		}

		client := pb.NewBrokerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.RegisterEntity(ctx, &pb.RegisterRequest{
			EntityId:   s.bankID,
			EntityType: pb.EntityType_BANK,
			Address:    listenAddr,
		})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("[%s] Registration error: %v. Retrying in 3s...\n", s.bankID, err)
			time.Sleep(3 * time.Second)
			continue
		}

		if !resp.Success {
			log.Printf("[%s] Registration rejected: %s. Retrying in 3s...\n", s.bankID, resp.Message)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("[%s] Registered successfully in Broker!\n", s.bankID)
		break
	}
}

func main() {
	bankID := flag.String("id", "BancoUSM", "Bank node identifier")
	port := flag.String("port", "50053", "Listening port")
	brokerAddr := flag.String("broker", "localhost:50051", "Broker address")
	flag.Parse()

	log.Printf("[%s] Starting USM Bank Server on port %s...\n", *bankID, *port)

	server := NewBancoServer(*bankID, *brokerAddr)

	// Get hostname to register
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	// Address that broker should use to call this bank.
	// In docker, use the service name which is lowercased id
	nodeIP := os.Getenv("NODE_IP")
	var listenAddr string
	if nodeIP != "" {
		listenAddr = fmt.Sprintf("%s:%s", nodeIP, *port)
	} else {
		containerAddr := fmt.Sprintf("%s:%s", strings.ToLower(*bankID), *port)
		if _, inDocker := os.LookupEnv("DOTENV"); inDocker || hostname != "localhost" {
			listenAddr = containerAddr
		} else {
			listenAddr = fmt.Sprintf("localhost:%s", *port)
		}
	}

	go server.registerInBroker(listenAddr)

	// Start listening
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBancoServiceServer(grpcServer, server)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
