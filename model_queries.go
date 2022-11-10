package simpledb

import (
	"database/sql"
	"errors"
	"strconv"
)

// Filter returns a QuerySet with the given filters applied.
func (d *Database) Filter(model Model, filter Filters, exclude []string) ModelSet {
	return d.FilterWithLimit(model, filter, d.LIMIT, exclude)
}

// See Filter.
// Also allows you to specify a limit on the number of results returned.
func (d *Database) FilterWithLimit(model Model, filters Filters, limit int, exclude []string) ModelSet {
	var query string = `SELECT * FROM ` + model.TableName()
	f_query, values := filters.Query(false)
	if f_query == "" {
		return nil
	}
	query += f_query
	query += " ORDER BY id DESC"
	query += ` LIMIT ` + strconv.Itoa(limit)
	results, err := d.Query(query, values...)
	if err != nil {
		panic(err)
		// return nil
	}
	return ScanRows(results, model, exclude)
}

// AllQ returns a query that will return all rows in the table.
func (d *Database) AllQ(model Model, exclude []string) string {
	cols := Columns(model)
	cols = Exclude(cols, exclude)
	query := "SELECT "
	for i, col := range cols {
		query += col
		if i < len(cols)-1 {
			query += ", "
		}
	}
	query += " FROM " + model.TableName()
	query += " ORDER BY id DESC"
	query += " LIMIT " + strconv.Itoa(d.LIMIT)
	return query
}

// Insert a model into the database.
// Takes a pointer to a model and a sql.Row and scans the ID of result into the model
func (d *Database) InsertModel(model Model) error {
	columns := Columns(model)
	values := make([]interface{}, len(columns))
	for i, column := range columns {
		values[i] = GetValue(model, column)
	}
	id, err := d.ExecInsert(model.TableName(), columns, values)
	if err != nil {
		return err
	}
	SetValue(model, "id", id)
	return nil
}

// Update a model in the database.
func (d *Database) UpdateModel(model Model) (Model, error) {
	columns := Columns(model)
	values := make([]interface{}, len(columns))
	for i, column := range columns {
		values[i] = GetValue(model, column)
	}
	id, err := d.ExecUpdate(model.TableName(), columns, values, "id = ?", GetValue(model, "id"))
	if err != nil {
		return nil, err
	}
	SetValue(model, "id", id)
	return model, nil
}

// All models from a table
func (d *Database) AllModel(model Model, exclude []string) ModelSet {
	query := d.AllQ(model, exclude)
	rows, err := d.Query(query)
	if err != nil {
		return nil
	}
	ms := ScanRows(rows, model, exclude)
	rows.Close()
	return ms
}

// Scan rows into models, put those into a ModelSet
func ScanRows(rows *sql.Rows, model Model, exclude []string) ModelSet {
	var models []Model
	for rows.Next() {
		model := NewModel(model)
		if err := Scan(model, rows, exclude); err != nil {
			panic(err)
		}
		models = append(models, model)
	}
	return models
}

// Scan a row into a model
func ScanRow(row *sql.Row, model Model, exclude []string) (Model, error) {
	model = NewModel(model)
	fields, err := modelFields(model, exclude)
	if err != nil {
		return nil, errors.New("modelFields: " + err.Error())
	}
	err = row.Scan(fields...)
	if err != nil {
		err = errors.New("no results found: " + err.Error())
	}
	return model, err
}
