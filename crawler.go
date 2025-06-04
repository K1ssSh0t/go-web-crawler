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

// Store handles persistence with in-memory cache
type Store struct {
	db      *sql.DB
	mu      sync.RWMutex
	visited map[string]bool // In-memory cache
}

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

func (s *Store) IsVisited(url string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.visited[url]
}

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

func (s *Store) Close() {
	s.db.Close()
}

// Crawler structure
type Crawler struct {
	store      *Store
	httpClient *http.Client
}

type task struct {
	url   string
	depth int
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

func (c *Crawler) extractLinks(doc *html.Node, base *url.URL) []string {
	var links []string
	linkSet := make(map[string]struct{})

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					link, err := url.Parse(attr.Val)
					if err != nil {
						continue
					}
					resolved := base.ResolveReference(link).String()
					// Filter out non-HTTP and fragments
					if strings.HasPrefix(resolved, "http") &&
						!strings.Contains(resolved, "#") {
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

	// Convert set to slice
	for link := range linkSet {
		links = append(links, link)
	}
	return links
}

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

func (c *Crawler) processURL(currentURL string, depth int, queue chan<- task, wg *sync.WaitGroup) {
	defer wg.Done()

	if depth <= 0 {
		return
	}

	// Verificar si ya fue visitado (con bloqueo de lectura)
	if c.store.IsVisited(currentURL) {
		return
	}

	// Procesamiento más rápido con timeout controlado
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc, err := c.fetchURLWithContext(ctx, currentURL)
	if err != nil {
		log.Printf("Fetch error: %s - %v", currentURL, err)
		return
	}

	base, _ := url.Parse(currentURL)
	title, content := c.extractContent(doc)
	links := c.extractLinks(doc, base)

	// Procesamiento en batch para mejor performance
	if err := c.store.AddSiteBatch(currentURL, title, content, links); err != nil {
		log.Printf("DB error: %v", err)
	}

	// Agregar links al queue en batch
	for _, link := range links {
		if !c.store.IsVisited(link) {
			wg.Add(1)
			queue <- task{url: link, depth: depth - 1}
		}
	}
}

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

// 4. Añadir método AddSiteBatch al Store
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
	crawler.crawl("https://example.com", 5)
	fmt.Printf("Crawled in %s\n", time.Since(start))
}
