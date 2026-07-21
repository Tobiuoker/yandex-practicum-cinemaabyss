package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

type MovieService struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewMovieService(baseURL string) *MovieService {
	return &MovieService{
		BaseURL:    baseURL,
		HTTPClient: http.DefaultClient,
	}
}

type App struct {
	monolithURL        string
	moviesServiceURL   string
	eventsServiceURL   string
	gradualMigration   bool
	migrationPercent   int
	monolithProxy      *httputil.ReverseProxy
	moviesServiceProxy *httputil.ReverseProxy
	eventsServiceProxy *httputil.ReverseProxy
}

func NewApp() *App {
	monolithURL := getEnv("MONOLITH_URL", "")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "")
	eventsServiceURL := getEnv("EVENTS_SERVICE_URL", "")
	isGradualMigrationEnabled := getEnv("GRADUAL_MIGRATION", "false") == "true"
	migrationPercent, err := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil {
		log.Fatalf("Failed to get migration percent: %v\n", err)
	}
	migrationPercent = clampPercent(migrationPercent)

	return &App{
		monolithURL:        monolithURL,
		moviesServiceURL:   moviesServiceURL,
		eventsServiceURL:   eventsServiceURL,
		gradualMigration:   isGradualMigrationEnabled,
		migrationPercent:   migrationPercent,
		monolithProxy:      newReverseProxy(monolithURL),
		moviesServiceProxy: newReverseProxy(moviesServiceURL),
		eventsServiceProxy: newOptionalReverseProxy(eventsServiceURL),
	}
}

func (app *App) Run(ctx context.Context) {
	http.HandleFunc("/health", app.handleHealth)
	http.HandleFunc("/api/movies", app.handleMovies)
	http.HandleFunc("/api/movies/health", app.handleMoviesHealth)
	http.HandleFunc("/api/events/", app.handleEvents)
	http.HandleFunc("/api/", app.handleMonolith)

	port := getEnv("PORT", "8000")
	log.Printf("Starting proxy service on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil && ctx.Err() == nil {
		log.Fatalf("Failed to start server: %v\n", err)
	}
}

func main() {
	NewApp().Run(context.Background())
}

func (app *App) handleMoviesHealth(w http.ResponseWriter, r *http.Request) {
	app.moviesServiceProxy.ServeHTTP(w, r)
}

func (app *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func (app *App) handleMovies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if r.URL.Query().Get("id") != "" {
			app.getMovieByID(w, r)
		} else {
			app.getAllMovies(w, r)
		}
	case "POST":
		app.createMovie(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (app *App) getMovieByID(w http.ResponseWriter, r *http.Request) {
	app.handleMoviesProxy(w, r)
}

func (app *App) createMovie(w http.ResponseWriter, r *http.Request) {
	app.handleMoviesProxy(w, r)
}

func (app *App) getAllMovies(w http.ResponseWriter, r *http.Request) {
	app.handleMoviesProxy(w, r)
}

func (app *App) handleMoviesProxy(w http.ResponseWriter, r *http.Request) {
	if app.useMoviesService() {
		app.moviesServiceProxy.ServeHTTP(w, r)
		return
	}
	app.monolithProxy.ServeHTTP(w, r)
}

func (app *App) handleMonolith(w http.ResponseWriter, r *http.Request) {
	app.monolithProxy.ServeHTTP(w, r)
}

func (app *App) handleEvents(w http.ResponseWriter, r *http.Request) {
	if app.eventsServiceProxy == nil {
		http.Error(w, "Events service is not configured", http.StatusServiceUnavailable)
		return
	}
	app.eventsServiceProxy.ServeHTTP(w, r)
}

func (app *App) useMoviesService() bool {
	if !app.gradualMigration {
		return false
	}
	return rand.IntN(100) < app.migrationPercent
}

func newOptionalReverseProxy(rawURL string) *httputil.ReverseProxy {
	if rawURL == "" {
		return nil
	}
	return newReverseProxy(rawURL)
}

func newReverseProxy(rawURL string) *httputil.ReverseProxy {
	target, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("Failed to parse service URL %q: %v\n", rawURL, err)
	}
	return httputil.NewSingleHostReverseProxy(target)
}

func clampPercent(percent int) int {
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
