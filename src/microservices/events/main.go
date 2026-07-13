package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

type App struct {
	writers map[string]*kafka.Writer
}

type Event struct {
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	app := &App{writers: map[string]*kafka.Writer{
		movieTopic:   newWriter(brokers, movieTopic),
		userTopic:    newWriter(brokers, userTopic),
		paymentTopic: newWriter(brokers, paymentTopic),
	}}
	defer app.close()

	ctx := context.Background()
	go consume(ctx, brokers, movieTopic)
	go consume(ctx, brokers, userTopic)
	go consume(ctx, brokers, paymentTopic)

	http.HandleFunc("/api/events/health", app.handleHealth)
	http.HandleFunc("/api/events/movie", app.handleMovieEvent)
	http.HandleFunc("/api/events/user", app.handleUserEvent)
	http.HandleFunc("/api/events/payment", app.handlePaymentEvent)

	port := getEnv("PORT", "8082")
	log.Printf("Starting events microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireOne,
	}
}

func consume(ctx context.Context, brokers []string, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "events-service",
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Failed to read %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("Consumed event from %s: key=%s value=%s", topic, string(msg.Key), string(msg.Value))
	}
}

func (app *App) close() {
	for _, writer := range app.writers {
		if err := writer.Close(); err != nil {
			log.Printf("Failed to close kafka writer: %v", err)
		}
	}
}

func (app *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func (app *App) handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	app.handleEvent(w, r, "movie", movieTopic)
}

func (app *App) handleUserEvent(w http.ResponseWriter, r *http.Request) {
	app.handleEvent(w, r, "user", userTopic)
}

func (app *App) handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	app.handleEvent(w, r, "payment", paymentTopic)
}

func (app *App) handleEvent(w http.ResponseWriter, r *http.Request, eventType, topic string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		Type:      eventType,
		CreatedAt: time.Now().UTC(),
	}
	value, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := app.writers[topic].WriteMessages(r.Context(), kafka.Message{
		Key:   []byte(eventType),
		Value: value,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
