package simpledb

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Connect to the database
func (db *Database) Connect() error {
	var err error
	db.conn, err = sql.Open("mysql", db.DSN())
	db.conn.SetConnMaxLifetime(time.Minute * 3)
	db.conn.SetMaxOpenConns(10)
	db.conn.SetMaxIdleConns(10)
	if err != nil {
		return err
	}
	db.Logger.Debug("Connected to database")
	return nil
}

// Close the database connection
func (db *Database) Close() error {
	db.Logger.Debug("Closing database connection")
	return db.conn.Close()
}

// Ping the database
func (db *Database) Ping() error {
	return db.conn.Ping()
}

// Query the database
func (db *Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db.Logger.Debug("QUERY: ", query, args)
	return db.conn.Query(query, args...)
}

// Query a database row
func (db *Database) QueryRow(query string, args ...interface{}) *sql.Row {
	db.Logger.Debug("QUERYROW: ", query, args)
	return db.conn.QueryRow(query, args...)
}

// Exec a query
func (db *Database) Exec(query string, args ...interface{}) (sql.Result, error) {
	db.Logger.Debug("EXEC: ", query, args)
	return db.conn.Exec(query, args...)
}

// Begin a transaction
func (db *Database) Begin() (*sql.Tx, error) {
	return db.conn.Begin()
}

// Prepare a query
func (db *Database) Prepare(query string) (*sql.Stmt, error) {
	return db.conn.Prepare(query)
}

// Prepare a query with context
func (db *Database) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.conn.PrepareContext(ctx, query)
}

// Exec a query with context
func (db *Database) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.conn.ExecContext(ctx, query, args...)
}

// Query the database with context
func (db *Database) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.conn.QueryContext(ctx, query, args...)
}

// Query the database row with context
func (db *Database) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.conn.QueryRowContext(ctx, query, args...)
}
