// Package main implements a concurrent web crawler with SQLite storage
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/html"
)

// Store handles the persistence of crawled pages and their relationships.
// It maintains thread-safe operations using a mutex and provides an in-memory
// cache of visited URLs for performance optimization.
type Store struct {
	db      *sql.DB          // SQLite database connection
	mu      sync.RWMutex     // Mutex for thread-safe operations
	visited map[string]bool   // In-memory cache of visited URLs
}

// loadVisited loads all previously visited URLs from the database into memory.
// This method rebuilds the in-memory cache during initialization to ensure
// consistency with the persistent storage.
// Returns an error if the database query fails.
func (s *Store) loadVisited() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Limpiar el mapa existente
	s.visited = make(map[string]bool)

	// Cargar URLs desde la base de datos
	rows, err := s.db.Query("SELECT url FROM sites")
	if err != nil {
		return fmt.Errorf("error querying visited URLs: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			log.Printf("Error scanning URL: %v", err)
			continue
		}
		s.visited[url] = true
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	log.Printf("Loaded %d visited URLs into cache", count)
	return nil
}

// NewStore creates a new Store instance with the specified SQLite database file.
// It initializes the database schema if it doesn't exist and loads visited URLs into memory.
// Parameters:
//   - dbFile: Path to the SQLite database file
// Returns:
//   - *Store: A new Store instance
//   - error: Any error that occurred during initialization
func NewStore(dbFile string) (*Store, error) {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, err
	}

	// Create tables
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS sites (
		url TEXT PRIMARY KEY,
		title TEXT,
		content TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE TABLE IF NOT EXISTS links (
		id INTEGER PRIMARY KEY,
		source TEXT,
		target TEXT,
		FOREIGN KEY(source) REFERENCES sites(url),
		FOREIGN KEY(target) REFERENCES sites(url)
	);
	
	CREATE INDEX IF NOT EXISTS idx_links_source ON links(source);
	CREATE INDEX IF NOT EXISTS idx_links_target ON links(target);
	`)
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:      db,
		visited: make(map[string]bool),
	}

	// Load existing URLs into cache
	rows, err := db.Query("SELECT url FROM sites")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			log.Printf("Error loading URL: %v", err)
			continue
		}
		store.visited[url] = true
	}

	log.Printf("Loaded %d visited URLs into cache", len(store.visited))
	return store, nil
}

// IsVisited checks if a URL has been already crawled by checking the in-memory cache.
// This method is thread-safe.
// Parameters:
//   - url: The URL to check
// Returns:
//   - bool: true if the URL has been visited, false otherwise
func (s *Store) IsVisited(url string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.visited[url]
}

// AddSite stores a crawled page and its outgoing links in the database.
// It updates both the persistent storage and the in-memory cache atomically
// using database transactions for data consistency.
// Parameters:
//   - url: The URL of the crawled page
//   - title: The page title
//   - content: The page content
//   - links: Slice of outgoing links found on the page
// Returns:
//   - error: Any error that occurred during storage
func (s *Store) AddSite(url, title, content string, links []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Already processed
	if s.visited[url] {
		return nil
	}

	// Start transaction
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert main page
	_, err = tx.Exec(`
		INSERT OR IGNORE INTO sites (url, title, content) 
		VALUES (?, ?, ?)`,
		url, title, content)
	if err != nil {
		return err
	}

	// Insert links
	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO links (source, target) 
		VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, link := range links {
		if _, err := stmt.Exec(url, link); err != nil {
			log.Printf("Link insert error: %v", err)
		}
	}

	// Update cache
	s.visited[url] = true

	return tx.Commit()
}

// Close closes the database connection.
func (s *Store) Close() {
	s.db.Close()
}

// Crawler represents the web crawler component that manages the crawling process.
// It coordinates the crawling of web pages, content extraction, and storage
// while maintaining concurrent operations.
type Crawler struct {
	store      *Store        // Database and cache store
	httpClient *http.Client  // Configured HTTP client for web requests
}

// task represents a URL to be crawled along with its depth in the crawling tree.
type task struct {
	url   string  // URL to be crawled
	depth int     // Remaining depth for crawling
}

func NewCrawler(store *Store) *Crawler {
	return &Crawler{
		store: store,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     5 * time.Second,
			},
		},
	}
}

// fetchURL retrieves the HTML content of a given URL using the configured HTTP client.
// Parameters:
//   - targetURL: The URL to fetch
// Returns:
//   - *html.Node: Parsed HTML document
//   - error: Any error that occurred during fetching
func (c *Crawler) fetchURL(targetURL string) (*html.Node, error) {
	resp, err := c.httpClient.Get(targetURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return html.Parse(resp.Body)
}

// extractContent extracts the title and main content from an HTML document.
// Parameters:
//   - doc: Parsed HTML document
// Returns:
//   - string: Page title
//   - string: Page content
func (c *Crawler) extractContent(doc *html.Node) (string, string) {
	var title, content string
	var f func(*html.Node)

	f = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "title":
				if n.FirstChild != nil {
					title = n.FirstChild.Data
				}
			case "p", "div", "article", "section":
				for child := n.FirstChild; child != nil; child = child.NextSibling {
					if child.Type == html.TextNode {
						content += child.Data + " "
					}
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			f(child)
		}
	}

	f(doc)
	return title, strings.Join(strings.Fields(content), " ")
}

// extractLinks extracts all valid HTTP(S) links from an HTML document.
// It resolves relative URLs against the base URL and removes duplicates.
// Parameters:
//   - doc: Parsed HTML document
//   - base: Base URL for resolving relative links
// Returns:
//   - []string: Slice of unique, absolute URLs
func (c *Crawler) extractLinks(doc *html.Node, base *url.URL) []string {
	 var links []string
    linkSet := make(map[string]struct{})

    log.Printf("Extrayendo enlaces de la página: %s", base.String())

    var f func(*html.Node)
    f = func(n *html.Node) {
        if n.Type == html.ElementNode && n.Data == "a" {
            for _, attr := range n.Attr {
                if attr.Key == "href" {
                    link, err := url.Parse(attr.Val)
                    if err != nil {
                        log.Printf("Error al parsear URL: %s - %v", attr.Val, err)
                        continue
                    }
                    resolved := base.ResolveReference(link).String()
                    if strings.HasPrefix(resolved, "http") && !strings.Contains(resolved, "#") {
                        log.Printf("Encontrado enlace válido: %s", resolved)
                        linkSet[resolved] = struct{}{}
                    }
                }
            }
        }
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			f(child)
		}
	}

	 f(doc)

    for link := range linkSet {
        links = append(links, link)
    }

    log.Printf("Extraídos %d enlaces únicos", len(links))
    return links
}

// crawl starts the crawling process from a given URL up to a maximum depth.
// It uses a concurrent worker pool to process URLs in parallel.
// Parameters:
//   - startURL: The URL to start crawling from
//   - maxDepth: Maximum depth to crawl
func (c *Crawler) crawl(startURL string, maxDepth int) {
	var wg sync.WaitGroup
	workQueue := make(chan task, 10000) // Buffer más grande
	defer close(workQueue)

	// Usar un rate limiter por worker en lugar de global
	workerCount := 50 // Más workers para mayor paralelismo

	// Inicializar workers
	for i := 0; i < workerCount; i++ {
		go func() {
			limiter := time.NewTicker(100 * time.Millisecond) // 10 req/segundo por worker
			defer limiter.Stop()

			for t := range workQueue {
				<-limiter.C
				c.processURL(t.url, t.depth, workQueue, &wg)
			}
		}()
	}

	// Cargar URLs visitadas desde DB solo una vez
	if len(c.store.visited) == 0 {
		if err := c.store.loadVisited(); err != nil {
			log.Printf("Error loading visited URLs: %v", err)
		}
	}

	wg.Add(1)
	workQueue <- task{url: startURL, depth: maxDepth}
	wg.Wait()
}

// processURL processes a single URL by fetching its content, extracting information,
// and storing it in the database. It also schedules newly discovered URLs for crawling.
// Parameters:
//   - currentURL: URL to process
//   - depth: Current crawling depth
//   - queue: Channel for scheduling new URLs
//   - wg: WaitGroup for synchronization
func (c *Crawler) processURL(currentURL string, depth int, queue chan<- task, wg *sync.WaitGroup) {
	defer wg.Done()

	// log.Printf("Procesando URL: %s (profundidad: %d)", currentURL, depth)

	if depth <= 0 {
		// log.Printf("Profundidad alcanzada, omitiendo: %s", currentURL)
		return
	}

	// Verificar si ya fue visitado (con bloqueo de lectura)
	if c.store.IsVisited(currentURL) {
		log.Printf("URL ya visitada, omitiendo: %s", currentURL)
		return
	}

	// log.Printf("Iniciando procesamiento de: %s", currentURL)

	// Procesamiento más rápido con timeout controlado
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc, err := c.fetchURLWithContext(ctx, currentURL)
	if err != nil {
		log.Printf("Error al obtener URL %s: %v", currentURL, err)
		return
	}

	base, _ := url.Parse(currentURL)
	title, content := c.extractContent(doc)
	link := c.extractLinks(doc, base)

	log.Printf("Extraídos %d enlaces de: %s", len(link), currentURL)

	// Procesamiento en batch para mejor performance
	if err := c.store.AddSiteBatch(currentURL, title, content, link); err != nil {
		log.Printf("Error al guardar en base de datos para %s: %v", currentURL, err)
	}

	// Agregar link al queue en batch
	for _, link := range link {
		// log.Printf("Encontrado enlace: %s", link)
		if !c.store.IsVisited(link) {
			wg.Add(1)
			queue <- task{url: link, depth: depth - 1}
			// log.Printf("Encolando nuevo enlace: %s (profundidad: %d)", link, depth-1)
		}
	}

	// log.Printf("Procesamiento completado para: %s", currentURL)
}

// fetchURLWithContext fetches a URL with context support for timeout control.
// Parameters:
//   - ctx: Context for timeout control
//   - url: URL to fetch
// Returns:
//   - *html.Node: Parsed HTML document
//   - error: Any error that occurred during fetching
func (c *Crawler) fetchURLWithContext(ctx context.Context, url string) (*html.Node, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return html.Parse(resp.Body)
}

// AddSiteBatch adds a site and its links to the database in a single transaction.
// This method provides better performance for batch operations.
// Parameters:
//   - url: The URL of the crawled page
//   - title: The page title
//   - content: The page content
//   - links: Slice of outgoing links
// Returns:
//   - error: Any error that occurred during storage
func (s *Store) AddSiteBatch(url, title, content string, links []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.visited[url] {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insertar sitio
	if _, err := tx.Exec(`INSERT OR IGNORE INTO sites (url, title, content) VALUES (?, ?, ?)`,
		url, title, content); err != nil {
		return err
	}

	// Insertar links en batch
	if len(links) > 0 {
		stmt, err := tx.Prepare(`INSERT OR IGNORE INTO links (source, target) VALUES (?, ?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, link := range links {
			if _, err := stmt.Exec(url, link); err != nil {
				log.Printf("Link insert error: %v", err)
			}
		}
	}

	s.visited[url] = true
	return tx.Commit()
}

func main() {
	store, err := NewStore("crawler.db")
	if err != nil {
		log.Fatal("Store init failed:", err)
	}
	defer store.Close()

	crawler := NewCrawler(store)
	start := time.Now()
	crawler.crawl("https://example.com", 2)
	fmt.Printf("Crawled in %s\n", time.Since(start))
}
