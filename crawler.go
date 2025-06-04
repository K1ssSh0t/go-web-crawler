package main

import (
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
			Timeout: 1 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     30 * time.Second,
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
	// workQueue := make(chan string, 1000)
	// 2. Modificar la cola de trabajo
	workQueue := make(chan task, 1000)
	rateLimiter := time.Tick(2 * time.Millisecond) // 5 req/sec

	wg.Add(1)
	// go func() { workQueue <- startURL }()

	// 3. Iniciar con profundidad máxima
	go func() { workQueue <- task{url: startURL, depth: maxDepth} }()

	// Worker pool
	for i := 0; i < 20; i++ {
		go func() {
			for t := range workQueue {
				<-rateLimiter
				// 4. En el worker
				// c.processURL(url, maxDepth, workQueue, &wg)
				c.processURL(t.url, t.depth, workQueue, &wg)
			}
		}()
	}

	wg.Wait()
	close(workQueue)
}

func (c *Crawler) processURL(currentURL string, depth int, queue chan<- task, wg *sync.WaitGroup) {
	defer wg.Done()

	if depth <= 0 || c.store.IsVisited(currentURL) {
		return
	}

	// Fetch and parse
	doc, err := c.fetchURL(currentURL)
	if err != nil {
		log.Printf("Fetch error: %s - %v", currentURL, err)
		return
	}

	// Extract content
	base, _ := url.Parse(currentURL)
	title, content := c.extractContent(doc)
	links := c.extractLinks(doc, base)

	// Persist to database
	if err := c.store.AddSite(
		currentURL,
		title,
		content,
		links,
	); err != nil {
		log.Printf("DB error: %v", err)
	}

	// Add new links to queue
	for _, link := range links {
		if !c.store.IsVisited(link) {
			wg.Add(1)
			go func(l string) {
				// queue <- l
				// 5. Al encolar nuevos links
				queue <- task{url: l, depth: depth - 1}  // Disminuir profundidad
			}(link)
		}
	}
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