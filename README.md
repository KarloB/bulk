```go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/karlob/bulk"
)

// Sample sample structure
type Sample struct {
	First  string
	Second string
	Third  int
}

var sampleData = []Sample{
	{"a", "b", 11251},
	{"c", "d", 22525},
	{"e", "f", 31255},
	{"g", "h", 51125},
	{"i", "j", 11252},
}

var query = "insert into sample (first, second, third) values (?, ?, ?)"

func main() {
	ctx := context.Background()
	var conn *sql.DB

	b := bulk.New(bulk.MySQLDB, conn)

	rows := make([]interface{}, len(sampleData))
	for i := range sampleData {
		rows[i] = sampleData[i]
	}
	err := b.BulkInsert(ctx, query, rows)
	if err != nil {
		log.Printf("BulkInsert: %v", err)
	}
}
```
