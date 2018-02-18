package bulk

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

const (
	insertService     = `insert into service (description, tag) values (?,?)`                                        // ok query
	insertServiceFull = `insert into service (id, description, tag)`                                                 // more columns than insert structure has
	emptyQuery        = ``                                                                                           // no statement at all
	invalidQuery      = `insert into service(id, (select something from somewhere where id = 'somethingelse'), tag)` // nested query
	invalidQuery2     = `insert into service`                                                                        // columns not defined
	invalidQuery3     = `insert into service values (?, ?)`                                                          // columns not defined
	invalidQuery4     = `insert into service (description, tag`                                                      // unclosed bracket
	invalidQuery5     = `insert into service (?, ?)`                                                                 // columns not defined
)

type TestService struct {
	Description string
	Tag         string
}

type losaStruktura struct {
}

const (
	expectedResponse = iota
	expectedError
)

func TestBulkInsertMySQL(t *testing.T) {
	var conn *sql.DB

	ctx := context.Background()

	b, err := New(MySQLDB, conn)
	if err != nil {
		t.Fatal(err)
	}

	numOfInserts := 150000
	rows := make([]interface{}, numOfInserts)
	for i := 0; i < numOfInserts; i++ {
		rows[i] = TestService{Description: fmt.Sprintf("Desc %d ", i), Tag: fmt.Sprintf("Tag %d", i)}
	}
	badRows := rows
	badRows = append(badRows, losaStruktura{})

	testData := []struct {
		request  []interface{}
		expected int
	}{
		{
			request:  nil,
			expected: expectedError,
		},
		{
			request:  badRows,
			expected: expectedError,
		},
		{
			request:  rows,
			expected: expectedError, // error starting transaction over sql mock
		},
	}

	for i := range testData {
		err := b.BulkInsert(ctx, insertService, testData[i].request)
		switch testData[i].expected {
		case expectedError:
			if err != nil {
				fmt.Println("Expected error: ", err)
			} else {
				t.Error("expected error")
			}
		case expectedResponse:
			if err != nil {
				t.Error(err)
			}
		}
	}
}

func TestBulkInsertOracle(t *testing.T) {
	var conn *sql.DB

	ctx := context.Background()
	b, err := New(OracleDB, conn)
	if err != nil {
		t.Fatal(err)
	}

	numOfInserts := 2002
	rows := make([]interface{}, numOfInserts)
	for i := 0; i < numOfInserts; i++ {
		rows[i] = TestService{Description: fmt.Sprintf("Desc %d ", i), Tag: fmt.Sprintf("Tag %d", i)}
	}

	testData := []struct {
		request  []interface{}
		expected int
	}{
		{
			request: rows,
			// expected: expectedResponse,
			expected: expectedError, // connection is nil
		},
		// expectedResponse
	}

	for i := range testData {
		err := b.BulkInsert(ctx, insertService, testData[i].request)
		switch testData[i].expected {
		case expectedError:
			if err == nil {
				t.Error("expected error")
			}
		case expectedResponse:
			if err != nil {
				t.Error(err)
			}
		}
	}
}

func TestMiscBadData(t *testing.T) {
	var conn *sql.DB

	ctx := context.Background()
	dbMySQL, err := New(MySQLDB, conn)
	if err != nil {
		t.Fatal(err)
	}
	dbOracle, err := New(OracleDB, conn)
	if err != nil {
		t.Fatal(err)
	}

	numOfInserts := 5
	rows := make([]interface{}, numOfInserts)
	for i := 0; i < numOfInserts; i++ {
		rows[i] = TestService{Description: fmt.Sprintf("Desc %d ", i), Tag: fmt.Sprintf("Tag %d", i)}
	}

	err = dbOracle.BulkInsert(ctx, insertServiceFull, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
	err = dbMySQL.BulkInsert(ctx, insertServiceFull, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
	err = dbOracle.BulkInsert(ctx, emptyQuery, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
	err = dbMySQL.BulkInsert(ctx, emptyQuery, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
	err = dbMySQL.BulkInsert(ctx, invalidQuery, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
	err = dbMySQL.BulkInsert(ctx, invalidQuery2, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
	err = dbMySQL.BulkInsert(ctx, invalidQuery3, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
	err = dbMySQL.BulkInsert(ctx, invalidQuery4, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
	err = dbMySQL.BulkInsert(ctx, invalidQuery5, rows)
	fmt.Println(err)
	if err == nil {
		t.Error("expected error")
	}
}
