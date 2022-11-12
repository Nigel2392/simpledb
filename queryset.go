package simpledb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Nigel2392/typeutils"
)

// Queryset is a struct that handles generating SQL queries
type QuerySet struct {
	Statements []string
	exclude    []string
	Filters    Filters
	Q          string
	db         *Database
	Model      Model
	OFFSET     int
	LIMIT      int
	PAGESIZE   int
}

// Initialize the QuerySet
func NewQuerySet(db *Database, model ...Model) *QuerySet {
	qs := &QuerySet{
		Statements: []string{},
		Filters:    Filters{},
		db:         db,
		LIMIT:      db.LIMIT,
		OFFSET:     0,
	}
	if len(model) > 0 {
		qs.Model = model[0]
	}
	return qs
}

// Add SQL statement to the QuerySet
func (q *QuerySet) Add(statement string) *QuerySet {
	q.Statements = append(q.Statements, statement)
	return q
}

// Add filters to apply in the where clause at the end of a query
func (q *QuerySet) AddFiters(filters Filters) *QuerySet {
	q.Filters = append(q.Filters, filters...)
	return q
}

// Generate the SQL query and values to be passed to the database
func (q *QuerySet) Query() (string, []interface{}) {
	f_query, values := q.Filters.Query(true)
	q.Q = strings.Join(q.Statements, " ") + f_query
	if q.PAGESIZE > 0 {
		q.Q += fmt.Sprintf(" LIMIT %d OFFSET %d", q.PAGESIZE, q.OFFSET)
	} else if q.LIMIT > 0 {
		q.Q += fmt.Sprintf(" LIMIT %d OFFSET %d", q.LIMIT, q.OFFSET)
	}
	return q.Q, values
}

// Clear the QuerySet
func (q *QuerySet) Clear() *QuerySet {
	q.Statements = []string{}
	q.Filters = Filters{}
	return q
}

// Get all models from the table
func (q *QuerySet) All() *QuerySet {
	q.Add(`SELECT *`)
	if q.Model != nil {
		q.From()
	}
	return q
}

// Count all models in the table
func (q *QuerySet) Count() *QuerySet {
	q.Add(`SELECT COUNT(*)`)
	return q
}

// Select specific columns from the table
func (q *QuerySet) Select(columns ...string) *QuerySet {
	for _, column := range columns {
		if !typeutils.Contains(q.exclude, column) {
			q.exclude = append(q.exclude, column)
		}
	}
	q.Add(fmt.Sprintf(`SELECT %s`, strings.Join(columns, ", ")))
	return q
}

// Group by a column
func (q *QuerySet) GroupBy(columns ...string) *QuerySet {
	q.Add(fmt.Sprintf(`GROUP BY %s`, strings.Join(columns, ", ")))
	return q
}

// Enter raw SQL into the end of the query
// Returns the sql query, and the values to be passed to the database
func (q *QuerySet) Raw(sql string) (string, []any) {
	q.Add(sql)
	return q.Query()
}

// Where adds a where clause to end of the query
func (q *QuerySet) Where(column string, op string, value any) *QuerySet {
	q.Filters = q.Filters.Add(column, op, value)
	return q
}

// From sets the table to query from
func (q *QuerySet) From(table ...string) *QuerySet {
	if len(table) > 0 {
		q.Add(fmt.Sprintf(`FROM %s`, table[0]))
	} else {
		if q.Model == nil {
			panic("no model provided, cannot infer table name")
		}
		q.Add(fmt.Sprintf(`FROM %s`, q.Model.TableName()))
	}
	return q
}

// OrderBy sets the order of the query
func (q *QuerySet) OrderBy(column string, order string) *QuerySet {
	q.Add(fmt.Sprintf(`ORDER BY %s %s`, column, order))
	return q
}

// Limit the number of results returned
func (q *QuerySet) Limit(limit int) *QuerySet {
	q.LIMIT = limit
	return q
}

// Offset the results returned
func (q *QuerySet) Offset(offset int) *QuerySet {
	q.OFFSET = offset
	return q
}

// Join a table to the query
func (q *QuerySet) Join(table string, column string, value any, op string) *QuerySet {
	q.Add(fmt.Sprintf(`JOIN %s ON %s %s %s`, table, column, op, value))
	return q
}

// Get a single model from the database
func (q *QuerySet) Get(values ...int) *QuerySet {
	q.setup()
	if len(values) > 0 {
		q.Where("id", "=", values[0])
	}
	q.Limit(1)
	return q
}

// Execute the query and return the results
func (q *QuerySet) Exec() (*sql.Rows, error) {
	query, vals := q.Query()
	return q.db.Query(query, vals...)
}

// Execute the query and return the results
func (q *QuerySet) ExecRow() (*sql.Row, error) {
	query, vals := q.Query()
	return q.db.QueryRow(query, vals...), nil
}

// Execute the query and return the results
func (q *QuerySet) ExecOne() (*sql.Row, error) {
	q.Limit(1)
	return q.ExecRow()
}

// Execute the query and return the results as a ModelSet
func (q *QuerySet) MultiModel(model ...Model) ModelSet {
	if len(model) > 0 {
		q.Model = model[0]
	}
	if q.Model == nil {
		panic("no model provided")
	}
	rows, err := q.Exec()
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	ms := ScanRows(rows, q.Model, q.exclude)
	rows.Close()
	return ms
}

// Execute the query and return the results as a Model
func (q *QuerySet) SingleModel(model ...Model) (Model, error) {
	if len(model) > 0 {
		q.Model = model[0]
	}
	if q.Model == nil {
		panic("no model provided")
	}
	row, err := q.ExecOne()
	if err != nil {
		return nil, errors.New("no results found: " + err.Error())
	}
	newmodel := NewModel(q.Model)
	return ScanRow(row, newmodel, q.exclude)
}

// Paginate the results
func (q *QuerySet) Page(page int) ModelSet {
	q.setup()
	q.Offset((page - 1) * q.PAGESIZE)
	return q.MultiModel()
}

// Setup a basic query, added so you don't have to type .All().From() every time.
func (q *QuerySet) setup() {
	if len(q.Statements) < 2 && q.Model != nil {
		q.Clear().All()
	} else if len(q.Statements) < 2 {
		panic("no model provided, cannot infer table name, please use GetFrom, or the from argument.")
	}
}
