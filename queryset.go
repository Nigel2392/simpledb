package simpledb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type QuerySet struct {
	Statements []string
	Filters    Filters
	Q          string
	db         *Database
	Model      Model
	OFFSET     int
	LIMIT      int
	PAGESIZE   int
}

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

func (q *QuerySet) Add(statement string) *QuerySet {
	q.Statements = append(q.Statements, statement)
	return q
}

func (q *QuerySet) AddFiters(filters Filters) *QuerySet {
	q.Filters = append(q.Filters, filters...)
	return q
}

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

func (q *QuerySet) Clear() *QuerySet {
	q.Statements = []string{}
	return q
}

func (q *QuerySet) All() *QuerySet {
	q.Add(`SELECT *`)
	if q.Model != nil {
		q.From()
	}
	return q
}

func (q *QuerySet) Count() *QuerySet {
	q.Add(`SELECT COUNT(*)`)
	return q
}

func (q *QuerySet) Select(columns ...string) *QuerySet {
	q.Add(fmt.Sprintf(`SELECT %s`, strings.Join(columns, ", ")))
	return q
}

func (q *QuerySet) GroupBy(columns ...string) *QuerySet {
	q.Add(fmt.Sprintf(`GROUP BY %s`, strings.Join(columns, ", ")))
	return q
}

func (q *QuerySet) Raw(sql string) (string, []any) {
	q.Add(sql)
	return q.Query()
}

func (q *QuerySet) Where(column string, op string, value any) *QuerySet {
	q.Filters = q.Filters.Add(column, op, value)
	return q
}

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

func (q *QuerySet) OrderBy(column string, order string) *QuerySet {
	q.Add(fmt.Sprintf(`ORDER BY %s %s`, column, order))
	return q
}

func (q *QuerySet) Limit(limit int) *QuerySet {
	q.LIMIT = limit
	return q
}

func (q *QuerySet) Offset(offset int) *QuerySet {
	q.OFFSET = offset
	return q
}

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

func (q *QuerySet) GetFrom(from, where, op string, value any) *QuerySet {
	q = q.Clear().All().From(from)
	q.Where(where, op, value)
	q.Limit(1)
	return q
}

func (q *QuerySet) Exec() (*sql.Rows, error) {
	query, vals := q.Query()
	return q.db.Query(query, vals...)
}

func (q *QuerySet) ExecRow() (*sql.Row, error) {
	query, vals := q.Query()
	return q.db.QueryRow(query, vals...), nil
}

func (q *QuerySet) ExecOne() (*sql.Row, error) {
	q.Limit(1)
	return q.ExecRow()
}

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
	ms := ScanRows(rows, q.Model)
	rows.Close()
	return ms
}

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
	return ScanRow(row, newmodel)
}

func (q *QuerySet) Page(page int) ModelSet {
	q.setup()
	q.Offset((page - 1) * q.PAGESIZE)
	return q.MultiModel()
}

func (q *QuerySet) setup() {
	if len(q.Statements) < 2 && q.Model != nil {
		q.Clear().All()
	} else if len(q.Statements) < 2 {
		panic("no model provided, cannot infer table name, please use GetFrom, or the from argument.")
	}
}
