package simpledb

import (
	"fmt"
	"os"
	"reflect"
	"sync"

	"github.com/Nigel2392/typeutils"
	"github.com/joho/godotenv"
)

// Get the address of the database
func (db *Database) Addr() string {
	return fmt.Sprintf("%s:%v", db.Host, db.Port)
}

// Get the DSN for the database
func (db *Database) DSN() string {
	if db.Password == "" {
		return fmt.Sprintf("%s@tcp(%s)/%s?parseTime=true", db.Username, db.Addr(), db.Database)
	}
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", db.Username, db.Password, db.Addr(), db.Database)
}

// Verify if database has enough credentials to initialize a connections.
func (db *Database) hasNoCredentials() bool {
	return db.Username == "" || db.Database == "" || db.Host == "" || db.Port == nil || db.Port == "" || db.SSL_MODE == ""
}

// Load the credentials from the .env file
func (db *Database) LoadCredentials() *Database {
	// Check if .env file exists
	if _, err := os.Stat(".env"); err == nil {
		// .env file exists, load it
		if err := godotenv.Load(); err != nil {
			panic(err)
		}
		// Get credentials from .env file
		db = db.GetFromEnv()
		// Check if credentials are still empty
		if db.hasNoCredentials() {
			panic("Database credentials are missing from .env file.")
		}
	} else {
		db = db.GetFromEnv()
		// .env file does not exist
		if db.hasNoCredentials() {
			panic("Database credentials are missing from environment variables.")
		}
	}
	return db
}

// Get the credentials from the environment variables
func (db *Database) GetFromEnv() *Database {
	db.Host = os.Getenv("DB_HOST")
	db.Port = os.Getenv("DB_PORT")
	db.Username = os.Getenv("DB_USERNAME")
	db.Password = os.Getenv("DB_PASSWORD")
	db.Database = os.Getenv("DB_NAME")
	db.SSL_MODE = os.Getenv("DB_SSLMODE")
	return db
}

// Convert a map to a slice of keys and a slice of values
func MapToSlice[T string | int | bool | int8 | int16 | int32 | int64](m map[T]any) ([]T, []any) {
	keys := make([]T, len(m))
	values := make([]any, len(m))
	i := 0
	for k, v := range m {
		keys[i] = k
		values[i] = v
		i++
	}
	return keys, values
}

// Shorthand for initializing asynchronous operations
func initSync(len int) (chan struct{}, *sync.WaitGroup, *sync.Mutex) {
	guard := make(chan struct{}, len)
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	return guard, &wg, &mu
}

// Exclude an item from a slice.
func Exclude[T comparable](a []T, b []T) []T {
	if len(a) == 0 || len(b) == 0 {
		return a
	} else if b == nil {
		return a
	}
	c := make([]T, 0)
	for _, v := range a {
		if !typeutils.Contains(b, v) {
			c = append(c, v)
		}
	}
	return c
}

// Get a new model struct from an existing model struct
func NewModel(model Model) Model {
	// Validate kind
	kind := modelKind(model)
	// Create a new instance of the model
	return reflect.New(kind).Interface().(Model)
}

// Do some setup before looping over the model fields.
func inlineLoopFields(kind reflect.Type, callback func(f reflect.StructField, i int)) {
	// Loop through all fields in the struct
	for i := 0; i < kind.NumField(); i++ {
		// Get the current field
		f := kind.Field(i)
		if !TagValid(f) {
			continue
		}
		callback(f, i)
	}
}

// Get the kind of the model (Reflect.TYPE)
func modelKind(model any) reflect.Type {
	// Validate kind
	kind := reflect.TypeOf(model)
	if kind.Kind() == reflect.Ptr {
		kind = kind.Elem()
	}
	if kind.Kind() != reflect.Struct {
		panic("model must be a struct")
	}
	return kind
}

// Convert a model to a table.
// Used for migrations
func ModelToTable(model Model) Table {
	table := Table{Name: model.TableName(), Columns: []Column{}}
	table.Columns = MigrationColumns(model)
	table.Relations = MigrationRelations(model)
	return table
}
