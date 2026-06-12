package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	pb "lab2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Consumidor struct {
	id          string
	payment     string
	brokerAddr  string
	csvPath     string
	ticketList  map[string]bool
	randSource  *rand.Rand
}

func NewConsumidor(id, payment, brokerAddr, csvDir string) *Consumidor {
	if csvDir != "" && csvDir != "." {
		os.MkdirAll(csvDir, 0755)
	}
	return &Consumidor{
		id:         id,
		payment:    payment,
		brokerAddr: brokerAddr,
		csvPath:    filepath.Join(csvDir, fmt.Sprintf("usuario_%s.csv", id)),
		ticketList: make(map[string]bool),
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Append ticket to CSV file
func (c *Consumidor) writeToCSV(ticket *pb.TicketInfo) error {
	file, err := os.OpenFile(c.csvPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// If file is new/empty, write header
	info, err := file.Stat()
	if err == nil && info.Size() == 0 {
		writer.Write([]string{"ticket_id", "event_id", "discoteca", "nombre_evento", "precio", "fecha_compra"})
	}

	record := []string{
		ticket.TicketId,
		ticket.EventId,
		ticket.Discoteca,
		ticket.NombreEvento,
		fmt.Sprintf("%d", ticket.Precio),
		ticket.FechaCompra,
	}

	return writer.Write(record)
}

func (c *Consumidor) registerAndRecover(client pb.BrokerServiceClient) {
	// 1. Register in Broker
	for {
		log.Printf("[%s] Registering at Broker...\n", c.id)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.RegisterEntity(ctx, &pb.RegisterRequest{
			EntityId:   c.id,
			EntityType: pb.EntityType_CONSUMER,
		})
		cancel()

		if err != nil {
			log.Printf("[%s] Registration error: %v. Retrying in 3s...\n", c.id, err)
			time.Sleep(3 * time.Second)
			continue
		}

		if !resp.Success {
			log.Printf("[%s] Registration rejected: %s. Retrying in 3s...\n", c.id, resp.Message)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("[%s] Registered successfully!\n", c.id)
		break
	}

	// 2. Recover history
	log.Printf("[%s] Requesting purchase history for recovery...\n", c.id)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := client.GetPurchaseHistory(ctx, &pb.HistoryRequest{ClientId: c.id})
	cancel()

	if err != nil {
		log.Printf("[%s] History recovery error: %v. Continuing...\n", c.id, err)
		return
	}

	recoveredCount := 0
	for _, ticket := range resp.Tickets {
		if !c.ticketList[ticket.TicketId] {
			c.ticketList[ticket.TicketId] = true
			if err := c.writeToCSV(ticket); err == nil {
				recoveredCount++
			}
		}
	}
	log.Printf("[%s] Recovery complete. Restored %d new tickets to CSV file (%d total in history).\n", c.id, recoveredCount, len(resp.Tickets))
}

func main() {
	id := flag.String("id", "ClienteA", "Consumer identifier")
	payment := flag.String("payment", "debito", "Payment method (debito/credito)")
	brokerAddr := flag.String("broker", "localhost:50051", "Broker address")
	csvDir := flag.String("csvdir", ".", "Directory to save CSV file")
	flag.Parse()

	log.Printf("[%s] Starting Consumidor (Payment: %s)...\n", *id, *payment)

	c := NewConsumidor(*id, *payment, *brokerAddr, *csvDir)

	var conn *grpc.ClientConn
	var brokerClient pb.BrokerServiceClient

	// Establish connection
	for {
		var err error
		conn, err = grpc.Dial(c.brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Connection to Broker failed: %v. Retrying in 3s...\n", c.id, err)
			time.Sleep(3 * time.Second)
			continue
		}
		brokerClient = pb.NewBrokerServiceClient(conn)
		break
	}
	defer conn.Close()

	// Register and do history recovery on startup
	c.registerAndRecover(brokerClient)

	// Shopping loop
	for {
		// Wait 10-20 seconds before attempting next purchase
		sleepSecs := c.randSource.Intn(11) + 10
		log.Printf("[%s] Next purchase check in %d seconds...\n", c.id, sleepSecs)
		time.Sleep(time.Duration(sleepSecs) * time.Second)

		// 1. Get available events
		log.Printf("[%s] Fetching available events...\n", c.id)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		eventsResp, err := brokerClient.GetEvents(ctx, &pb.GetEventsRequest{ClientId: c.id})
		cancel()

		if err != nil {
			log.Printf("[%s] Error getting events: %v. Retrying next loop...\n", c.id, err)
			continue
		}

		if len(eventsResp.Events) == 0 {
			log.Printf("[%s] No events available at the moment.\n", c.id)
			continue
		}

		// 2. Select one event randomly
		selectedIdx := c.randSource.Intn(len(eventsResp.Events))
		selectedEvent := eventsResp.Events[selectedIdx]

		log.Printf("[%s] Selected event: ID=%s, Name=%s, Price=%d, Available Stock=%d\n",
			c.id, selectedEvent.EventId, selectedEvent.NombreEvento, selectedEvent.Precio, selectedEvent.Stock)

		// 3. Purchase Ticket
		log.Printf("[%s] Requesting purchase for event %s...\n", c.id, selectedEvent.EventId)
		buyCtx, buyCancel := context.WithTimeout(context.Background(), 5*time.Second)
		buyResp, err := brokerClient.BuyTicket(buyCtx, &pb.BuyRequest{
			ClientId:  c.id,
			EventId:   selectedEvent.EventId,
			MedioPago: c.payment,
			Monto:     selectedEvent.Precio,
		})
		buyCancel()

		if err != nil {
			log.Printf("[%s] Purchase request failed: %v\n", c.id, err)
			continue
		}

		if buyResp.Success {
			log.Printf("[%s] PURCHASE APPROVED! Ticket ID: %s\n", c.id, buyResp.TicketId)
			c.ticketList[buyResp.TicketId] = true

			// Log to CSV
			ticket := &pb.TicketInfo{
				TicketId:     buyResp.TicketId,
				ClientId:     c.id,
				EventId:      selectedEvent.EventId,
				Discoteca:    selectedEvent.Discoteca,
				NombreEvento: selectedEvent.NombreEvento,
				Precio:       selectedEvent.Precio,
				FechaCompra:  time.Now().Format(time.RFC3339),
			}
			if err := c.writeToCSV(ticket); err != nil {
				log.Printf("[%s] Error writing ticket to CSV: %v\n", c.id, err)
			}
		} else {
			log.Printf("[%s] PURCHASE REJECTED: %s\n", c.id, buyResp.Message)
		}
	}
}
