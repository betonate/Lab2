package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CatalogEvent struct {
	EventoID         string `json:"evento_id"`
	Discoteca        string `json:"discoteca"`
	NombreEvento     string `json:"nombre_evento"`
	Categoria        string `json:"categoria"`
	Comuna           string `json:"comuna"`
	Precio           int32  `json:"precio"`
	Stock            int32  `json:"stock"`
	FechaEvento      string `json:"fecha_evento"`
	FechaPublicacion string `json:"fecha_publicacion"`
}

func main() {
	name := flag.String("name", "DataClub", "Productor / Discoteca name")
	catalogPath := flag.String("catalog", "tests_files/catalogo_eventos_30.json", "Catalog JSON file path")
	brokerAddr := flag.String("broker", "localhost:50051", "Broker address")
	flag.Parse()

	log.Printf("[%s] Starting Productor...\n", *name)

	// Read catalog
	file, err := os.Open(*catalogPath)
	if err != nil {
		log.Fatalf("Error opening catalog file: %v", err)
	}
	defer file.Close()

	byteValue, _ := io.ReadAll(file)
	var allEvents []CatalogEvent
	if err := json.Unmarshal(byteValue, &allEvents); err != nil {
		log.Fatalf("Error parsing catalog JSON: %v", err)
	}

	// Filter events for this discoteca
	var myTemplates []CatalogEvent
	for _, ev := range allEvents {
		if strings.EqualFold(ev.Discoteca, *name) {
			myTemplates = append(myTemplates, ev)
		}
	}

	if len(myTemplates) == 0 {
		log.Printf("[%s] WARNING: No events found in catalog for this discoteca!\n", *name)
		// Let's create a dummy template if empty to prevent crash
		myTemplates = append(myTemplates, CatalogEvent{
			EventoID:     "EV-DUMMY",
			Discoteca:    *name,
			NombreEvento: "Fiesta General " + *name,
			Categoria:    "VIP",
			Comuna:       "Santiago",
			Precio:       10000,
			Stock:        100,
			FechaEvento:  "2025-12-31T23:59:59",
		})
	}

	log.Printf("[%s] Found %d event templates in catalog.\n", *name, len(myTemplates))

	// Register in Broker
	var conn *grpc.ClientConn
	for {
		log.Printf("[%s] Connecting to Broker at %s...\n", *name, *brokerAddr)
		var err error
		conn, err = grpc.Dial(*brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Failed to connect to broker: %v. Retrying in 3s...\n", *name, err)
			time.Sleep(3 * time.Second)
			continue
		}

		client := pb.NewBrokerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.RegisterEntity(ctx, &pb.RegisterRequest{
			EntityId:   *name,
			EntityType: pb.EntityType_PRODUCER,
		})
		cancel()

		if err != nil {
			log.Printf("[%s] Registration error: %v. Retrying in 3s...\n", *name, err)
			conn.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		if !resp.Success {
			log.Printf("[%s] Registration rejected: %s. Retrying in 3s...\n", *name, resp.Message)
			conn.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("[%s] Registered successfully in Broker!\n", *name)
		break
	}
	defer conn.Close()

	brokerClient := pb.NewBrokerServiceClient(conn)
	rSource := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Main loop: publish events periodically
	templateIdx := 0
	for {
		// Select next event template
		template := myTemplates[templateIdx]
		templateIdx = (templateIdx + 1) % len(myTemplates)

		// Generate variations: decrement stock, vary price
		// E.g., stock between 50 and 300, price varies dynamic
		var stock int32
		if template.Stock > 10 {
			stock = rSource.Int31n(template.Stock-10) + 10 // stock > 0
		} else {
			stock = template.Stock
		}
		priceFactor := 1.0 + (1.0-float64(stock)/float64(template.Stock))*0.3 // up to 30% increase
		price := int32(float64(template.Precio) * priceFactor)

		// Create unique ID for this instance (idempotency token)
		instanceID := fmt.Sprintf("%s-%d", template.EventoID, time.Now().UnixNano())

		eventToPublish := &pb.Event{
			EventId:          instanceID,
			Discoteca:        *name,
			NombreEvento:     template.NombreEvento,
			Categoria:        template.Categoria,
			Comuna:           template.Comuna,
			Precio:           price,
			Stock:            stock,
			FechaEvento:      template.FechaEvento,
			FechaPublicacion: time.Now().Format(time.RFC3339),
		}

		log.Printf("[%s] Publishing event: ID=%s, Name=%s, Price=%d, Stock=%d\n", *name, instanceID, eventToPublish.NombreEvento, price, stock)

		// Send with retry (Idempotency check)
		success := false
		for retries := 0; retries < 3; retries++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := brokerClient.PublishEvent(ctx, eventToPublish)
			cancel()

			if err != nil {
				log.Printf("[%s] Publish error: %v. Retrying in 2s...\n", *name, err)
				time.Sleep(2 * time.Second)
				continue
			}

			if resp.Success {
				log.Printf("[%s] Event published successfully: %s\n", *name, resp.Message)
				success = true
				break
			} else {
				log.Printf("[%s] Publish rejected: %s. Proceeding to next.\n", *name, resp.Message)
				break
			}
		}

		if !success {
			log.Printf("[%s] Failed to publish event %s after retries.\n", *name, instanceID)
		}

		// Random sleep between 30 and 40 seconds
		sleepTime := time.Duration(rSource.Intn(11)+30) * time.Second
		log.Printf("[%s] Next publication in %v...\n", *name, sleepTime)
		time.Sleep(sleepTime)
	}
}
