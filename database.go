package simpledb

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/Nigel2392/simplelog"
	"github.com/go-sql-driver/mysql"
)

// ENVIRONMENT VARIABLES:
// These can be set with a .env file in the root of the project,
// or by setting them in the environment variables of the system.
// DB_HOST
// DB_PORT
// DB_USERNAME
// DB_PASSWORD (optional)
// DB_NAME (database name)
// DB_SSLMODE (disable, prefer, require, verify-ca, verify-full)

// Equality operators for use in filters
const (
	IN   = "IN"
	EQ   = "="
	NE   = "!="
	GT   = ">"
	GTE  = ">="
	LT   = "<"
	LTE  = "<="
	LIKE = "LIKE"
)

// Database is the main struct for the database
type Database struct {
	Host            string
	Port            any
	Username        string
	Password        string `json:"-"`
	Database        string
	SSL_MODE        string
	LIMIT           int
	conn            *sql.DB          `json:"-"`
	models          []Model          `json:"-"`
	LatestMigration *Migration       `json:"-"`
	Logger          simplelog.Logger `json:"-"`
}

// Connect to the database
func NewDatabase(loglevel ...string) *Database {
	latest_migration, _ := Migration{}.GetLatestMigration()
	var logger simplelog.Logger
	if len(loglevel) > 0 {
		logger = simplelog.NewLogger(loglevel[0])
	} else {
		logger = simplelog.NewLogger("info")
	}
	return &Database{
		Host:            "",
		Port:            "",
		Username:        "",
		Password:        "",
		Database:        "",
		LIMIT:           1000,
		LatestMigration: &latest_migration,
		Logger:          logger,
	}
}

// Database string method
func (db *Database) String() string {
	return fmt.Sprintf("Database: %s", db.Database)
}

// Register a model with the database
// This will be used to create the table if it doesn't exist,
// and to create the migration if it doesn't exist
func (db *Database) Register(model Model) {
	db.Logger.Debug("Registering model: " + model.TableName())
	db.models = append(db.models, model)
}

// Migrate the database to the latest version
func (db *Database) Migrate() error {
	db.Logger.Info("Initializing migration")
	migration := NewMigration(db)
	migration.CreateFromModels(db.models)
	return migration.Run()
}

// Shorthand for a queryset
func (db *Database) NewQS(mdl Model) *QuerySet {
	return NewQuerySet(db, mdl)
}

// Model interface
type Model interface {
	TableName() string
}

// Check for SQL errors
func (db *Database) CheckError(err error, number int) bool {
	return CheckSQLError(err, number)
}

// Check for SQL errors
func CheckSQLError(err error, number int) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}
	return false
}

// Return all the models registered with the database
func (db *Database) AllModels() []Model {
	return db.models
}
