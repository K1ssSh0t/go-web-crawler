# Go Web Crawler

A concurrent web crawler with SQLite storage written in Go.

## Features

- Concurrent crawling with worker pool
- SQLite storage for crawled pages and link relationships
- In-memory URL cache for performance
- Rate limiting per worker
- Context-aware HTTP requests with timeouts
- Content extraction (title and main text)
- Link extraction with URL normalization

## Requirements

- Go 1.24.1 or higher
- SQLite3 (development headers)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/go-web-crawler.git
cd go-web-crawler
```

2. Install dependencies:

```bash
go mod download
```

## Usage

Run the crawler with default settings:

```bash
go run crawler.go
```

### Configuration

Modify the following parameters in `crawler.go`:

- `maxDepth`: Controls how deep the crawler will go (default: 2)
- `workerCount`: Number of concurrent workers (default: 50)
- `rateLimit`: Delay between requests per worker (default: 100ms)
- `startURL`: Initial URL to crawl (default: "https://example.com")

## Database Schema

The crawler uses SQLite with the following schema:

```sql
CREATE TABLE sites (
    url TEXT PRIMARY KEY,
    title TEXT,
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE links (
    id INTEGER PRIMARY KEY,
    source TEXT,
    target TEXT,
    FOREIGN KEY(source) REFERENCES sites(url),
    FOREIGN KEY(target) REFERENCES sites(url)
);
```

## Performance Considerations

- Uses batch operations for database writes
- Implements read/write locks for thread safety
- Maintains in-memory URL cache to avoid duplicate requests
- Configurable rate limiting per worker

## License

MIT License - see [LICENSE](LICENSE) file (to be added)
