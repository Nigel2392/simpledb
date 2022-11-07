package simpledb

import (
	"database/sql"
	"errors"
	"strconv"
)

func (d *Database) Filter(model Model, filter Filters) ModelSet {
	return d.FilterWithLimit(model, filter, d.LIMIT)
}

func (d *Database) FilterWithLimit(model Model, filters Filters, limit int) ModelSet {
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
	return ScanRows(results, model)
}

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

func (d *Database) AllModel(model Model, exclude []string) ModelSet {
	query := d.AllQ(model, exclude)
	rows, err := d.Query(query)
	if err != nil {
		return nil
	}
	ms := ScanRows(rows, model)
	rows.Close()
	return ms
}

func ScanRows(rows *sql.Rows, model Model) ModelSet {
	var models []Model
	for rows.Next() {
		model := NewModel(model)
		if err := Scan(model, rows); err != nil {
			panic(err)
		}
		models = append(models, model)
	}
	return models
}

func ScanRow(row *sql.Row, model Model) (Model, error) {
	model = NewModel(model)
	fields, err := modelFields(model)
	if err != nil {
		return nil, errors.New("modelFields: " + err.Error())
	}
	err = row.Scan(fields...)
	if err != nil {
		err = errors.New("no results found: " + err.Error())
	}
	return model, err
}
