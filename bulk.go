package bulk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Bulk package descriptor
// contains database connection and database type
type Bulk struct {
	dbType DatabaseType
	conn   *sql.DB
}

// DatabaseType database type
// this is important, since databases Oracle bulk insert is way different than MySQL one
type DatabaseType int

const (
	// MySQLDB database is MySQL
	MySQLDB DatabaseType = iota + 1
	// OracleDB database is Oracle
	OracleDB
)

// one statement can not have more placeholders, thus they are limited
const mysqlMaxPlaceholders = 65535
const oracleMaxPlaceholders = 1000

// New init bulk insert
// define database type (MySQL or Oracle) and provide database connection to init bulk insert
func New(dbType DatabaseType, conn *sql.DB) (*Bulk, error) {
	t := &Bulk{
		conn:   conn,
		dbType: dbType,
	}

	return t, nil
}

// queryArgs query + args pairs
type queryArgs struct {
	query string
	args  []interface{}
}

// BulkInsert will transform query into bulk insert variation and execute created queries in transaction
//
// for three entries in slice, MySQL query
//  insert into something values (?, ?)
// will become:
//  insert into something values (?, ?), (?, ?), (?, ?)
// rows  must be structures of same type, otherwise checker function will return an error
func (t *Bulk) BulkInsert(ctx context.Context, query string, rows []interface{}) error {
	err := t.checkInsertRequest(query, rows)
	if err != nil {
		return err
	}

	chunks := chunkIt(rows, t.dbType)
	insertData := make([]queryArgs, len(chunks))

	switch t.dbType {
	case OracleDB:
		for i := range chunks {
			insertData[i].query, insertData[i].args, err = createStatementOracle(query, chunks[i])
			if err != nil {
				return err
			}
		}
	default: // MySQLDB
		for i := range chunks {
			insertData[i].query, insertData[i].args, err = createStatementMySQL(query, chunks[i])
			if err != nil {
				return err
			}
		}
	}

	if t.conn == nil {
		return errors.New("DB Connection is nil")
	}

	if insertData != nil && len(insertData) > 0 {
		tx, err := t.conn.Begin()
		if err != nil {
			return errors.New("Error staring transaction")
		}
		for i := range insertData {
			_, err = tx.Exec(insertData[i].query, insertData[i].args...)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
		err = tx.Commit()
		if err != nil {
			return errors.New("Error transaction commit")
		}
	}

	return nil
}

// CreateStatementMySQL create bulk insert statement for MySQL database based on provided arguments
func createStatementMySQL(query string, rows []interface{}) (string, []interface{}, error) {
	var err error
	placeholder, count, err := createPlaceholder(query, rows[0])
	if err != nil {
		return "", nil, err
	}

	placeholders := make([]string, len(rows))
	args := make([]interface{}, (len(rows) * count))

	var argCount int
	for i, entry := range rows {
		placeholders[i] = placeholder
		v := reflect.ValueOf(entry)
		for y := 0; y < v.NumField(); y++ {
			args[argCount] = v.Field(y).Interface()
			argCount++
		}
	}

	query = queryValuesStrip(query)
	statement := fmt.Sprintf("%s %s", query, strings.Join(placeholders, ","))
	return statement, args, nil
}

// CreateStatementOracle create bulk insert statement for Oracle database based on provided arguments
func createStatementOracle(query string, rows []interface{}) (string, []interface{}, error) {
	placeholder, lenCols, err := createPlaceholder(query, rows[0])
	if err != nil {
		return "", nil, err
	}
	s := reflect.ValueOf(rows)
	lenRows := s.Len()

	query = queryValuesStrip(query)
	query = strings.Replace(query, "insert", "", 1)

	wrapQuery := func(q string) string {
		before := "insert all"
		after := "select * from dual"
		return fmt.Sprintf("%s %s %s", before, q, after)
	}

	queries := make([]string, lenRows)
	args := make([]interface{}, lenRows*lenCols)

	var argCount int
	for i := range rows {
		queries[i] = fmt.Sprintf("%s %s", query, placeholder)
		v := reflect.ValueOf(rows[i])
		for y := 0; y < v.NumField(); y++ {
			args[argCount] = v.Field(y).Interface()
			argCount++
		}
	}

	mergedQuery := strings.Join(queries, " ")
	for i := 0; i < lenRows*lenCols; i++ {
		mergedQuery = strings.Replace(mergedQuery, "?", fmt.Sprintf(":%d", i), 1)
	}

	statement := wrapQuery(mergedQuery)
	statement = removeDoubleSpace(statement)

	return statement, args, nil
}

// checkInsertRequest does various tests for insert statement and provided slice of data
func (t *Bulk) checkInsertRequest(query string, rows []interface{}) error {

	switch t.dbType {
	case MySQLDB, OracleDB:
	default:
		return errors.New("Database type not defined")
	}

	if rows == nil {
		return errors.New("Invalid value for rows")
	}
	if len(rows) == 0 {
		return errors.New("No rows in request")
	}
	for i := range rows {
		if rows[i] == nil {
			return errors.New("Row is nil")
		}
		if i > 0 {
			if reflect.TypeOf(rows[i]) != reflect.TypeOf(rows[i-1]) {
				return fmt.Errorf("Invalid type for row %d", i)
			}
		}
	}
	if len(query) == 0 {
		return errors.New("Query is empty")
	}
	return nil
}

// ChunkIt splits slice into slices of slice based on batch size
// for example, single oracle statement can not have more than 1000 placeholders, thus we split big slice into smaller slices or batches
func chunkIt(rows []interface{}, dbType DatabaseType) [][]interface{} {
	instance := reflect.TypeOf(rows[0])
	fCount := instance.NumField()

	var maxBatch int
	switch dbType {
	case OracleDB:
		maxBatch = oracleMaxPlaceholders / fCount
	default: // MySQLDB
		maxBatch = mysqlMaxPlaceholders / fCount
	}
	batchSize := len(rows)
	if batchSize > maxBatch {
		batchSize = findBatchSize(batchSize, maxBatch)
	}

	rowLen := len(rows)
	rowChunk := batchSize

	var result [][]interface{}
	if rowLen > rowChunk {
		for i := 0; i < len(rows); i += rowChunk {
			end := i + rowChunk
			if end > len(rows) {
				end = len(rows)
			}
			result = append(result, rows[i:end])
		}
	} else {
		result = append(result, rows)
	}

	return result
}

// findBatchSize find batch size up to the limit
func findBatchSize(a int, limit int) int {
	var result int
	i := 1
	for {
		result = int(a / i)
		if result < limit {
			break
		}
		i++
	}
	return result
}

// removeDoubleSpace remove double whitespace from query string
func removeDoubleSpace(a string) string {
	return strings.Replace(a, "  ", " ", -1)
}

// queryValuesStrip delete values... from query string
func queryValuesStrip(query string) string {
	valuesIndex := strings.Index(query, "values")
	if valuesIndex > 0 {
		query = query[:valuesIndex] // delete placeholders if any exist
	}

	query = fmt.Sprintf("%s values", query)
	return query
}

// createPlaceholder create placeholder for one insert on structure. Check if placeholder matches query column count. Returns placeholder, column count, error
func createPlaceholder(query string, a interface{}) (string, int, error) {
	instance := reflect.TypeOf(a)
	fCount := instance.NumField()

	columns, err := extractQueryColumns(query)
	if err != nil {
		return "", 0, err
	}

	if len(columns) != fCount {
		return "", 0, fmt.Errorf("Structure type doesn't match column count")
	}

	ph := make([]string, fCount)
	for i := 0; i < fCount; i++ {
		ph[i] = "?"
	}

	placeholder := fmt.Sprintf("(%s)", strings.Join(ph, ","))

	return placeholder, fCount, nil
}

func extractQueryColumns(query string) ([]string, error) {
	columnsStart := strings.Index(query, "(")
	columnsEnd := strings.Index(query, ")")

	if columnsStart < 0 || columnsEnd < 0 {
		return nil, fmt.Errorf("Query columns not properly defined. Query: %s", query)
	}

	columnsString := query[columnsStart+1 : columnsEnd]
	columnsString = strings.Replace(columnsString, " ", "", -1)
	columns := strings.Split(columnsString, ",")

	for i := range columns {
		if columns[i] == "?" {
			return nil, fmt.Errorf("Invalid column name: %s", columns[i])
		}
	}

	return columns, nil
}
