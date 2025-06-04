package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/html"
)

// Track visited URLs
var visited = make(map[string]bool)
var mu sync.Mutex

// Add new URLDepth struct to track depth
type URLDepth struct {
	url   string
	depth int
}

func fetchURL(targetURL string) (*html.Node, error) {
	resp, err := http.Get(targetURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code error: %d %s", resp.StatusCode, resp.Status)
	}

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func extractLinks(doc *html.Node, base *url.URL) []string {
	var links []string
	var f func(*html.Node)
	
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					link, err := url.Parse(attr.Val)
					if err != nil {
						continue
					}
					resolvedLink := base.ResolveReference(link).String()
					links = append(links, resolvedLink)
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	
	f(doc)
	return links
}

// Replace crawl function
func crawl(startURL string, maxDepth int) {
    var wg sync.WaitGroup
    workQueue := make(chan URLDepth, 100)
    rateLimiter := time.NewTicker(2 * time.Millisecond)
    defer rateLimiter.Stop()

    // Start with the initial URL
    wg.Add(1)
    workQueue <- URLDepth{startURL, maxDepth}

    // Create worker pool
    const numWorkers = 5
    for i := 0; i < numWorkers; i++ {
        go func() {
            for urlDepth := range workQueue {
                <-rateLimiter.C // Rate limiting
                processURL(urlDepth.url, urlDepth.depth, workQueue, &wg)
            }
        }()
    }

    wg.Wait()
    close(workQueue)
}

// Replace processURL function
func processURL(currentURL string, depth int, queue chan<- URLDepth, wg *sync.WaitGroup) {
    defer wg.Done()

    // Check depth
    if depth <= 0 {
        return
    }

    // Parse URL first
    parsedURL, err := url.Parse(currentURL)
    if err != nil {
        fmt.Printf("Error parsing URL %s: %v\n", currentURL, err)
        return
    }

    // Skip non-HTTP(S) URLs
    if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
        return
    }

    // Check if already visited
    mu.Lock()
    if visited[currentURL] {
        mu.Unlock()
        return
    }
    visited[currentURL] = true
    mu.Unlock()

    fmt.Printf("Crawling [Depth: %d]: %s\n",depth, currentURL)

    // Fetch page
    doc, err := fetchURL(currentURL)
    if err != nil {
        fmt.Printf("Error fetching %s: %v\n", currentURL, err)
        return
    }

    // Extract links
    links := extractLinks(doc, parsedURL)

    // Process new links
    for _, link := range links {
        mu.Lock()
        if !visited[link] {
            wg.Add(1)
            queue <- URLDepth{link, depth - 1}
        }
        mu.Unlock()
    }
}

// Replace main function
func main() {
    start := time.Now()
    maxDepth := 3 // Reduced depth for testing
    startURL := "https://example.com" // Change to a valid URL for testing
    
    fmt.Printf("Starting crawl from %s with max depth %d\n", startURL, maxDepth)
    crawl(startURL, maxDepth)
    
    fmt.Printf("\nCrawling completed in: %s\n", time.Since(start))
    fmt.Printf("Visited %d URLs:\n", len(visited))
    
    // Print results in a sorted manner
    var urls []string
    for url := range visited {
        urls = append(urls, url)
    }
    sort.Strings(urls)
    for _, url := range urls {
        fmt.Println(url)
    }
}