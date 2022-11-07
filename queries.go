package simpledb

import (
	"database/sql"
)

func (d *Database) DB_Tables() ([]string, error) {
	rows, err := d.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (db *Database) GetColumnValue(model Model, col string, id any) interface{} {
	query := "SELECT " + col + " FROM " + model.TableName() + " WHERE id = ?"
	var value interface{}
	err := db.QueryRow(query, id).Scan(&value)
	if err != nil {
		return nil
	}
	return value
}

func (db *Database) DB_Columns(table_name string) ([]string, error) {
	rows, err := db.Query("SELECT column_name FROM information_schema.columns WHERE table_name = '" + table_name + "'")
	if err != nil {
		return nil, err
	}
	var columns []string
	for rows.Next() {
		var column string
		err = rows.Scan(&column)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func (db *Database) DB_Columns_With_Type(table_name string) (map[string]string, error) {
	rows, err := db.Query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + table_name + "'")
	if err != nil {
		return nil, err
	}
	var columns = make(map[string]string)
	for rows.Next() {
		var column string
		var data_type string
		err = rows.Scan(&column, &data_type)
		if err != nil {
			return nil, err
		}
		columns[column] = data_type
	}
	return columns, nil
}

func (db *Database) DB_ColumnTypes(table_name string) ([]*sql.ColumnType, error) {
	rows, err := db.Query("SELECT * FROM " + table_name)
	if err != nil {
		return nil, err
	}
	return rows.ColumnTypes()
}

func (db *Database) Count(table_name string, filter ...Filter) (int, error) {
	var count int
	var query string = `SELECT COUNT(*) FROM ` + table_name
	var values []interface{}
	if len(filter) > 0 {
		query += ` WHERE`
		for i, f := range filter {
			if i > 0 {
				query += ` AND`
			}
			if f.Operator == "" {
				f.Operator = "="
			}
			switch f.Operator {
			case "IN", "in":
				query += ` ` + f.Column + ` IN (`
				for j, v := range f.Value.([]interface{}) {
					query += `?`
					if j < len(f.Value.([]interface{}))-1 {
						query += `,`
					}
					values = append(values, v)
				}
				query += `)`
			default:
				query += ` ` + f.Column + ` ` + f.Operator + ` ?`
				values = append(values, f.Value)
			}
		}
	}
	err := db.QueryRow(query, values...).Scan(&count)
	return count, err
}

func (db *Database) DB_ColumnType(table_name string, column_name string) ([]*sql.ColumnType, error) {
	rows, err := db.Query("SELECT " + column_name + " FROM " + table_name)
	if err != nil {
		return nil, err
	}
	return rows.ColumnTypes()
}

func (db *Database) DropTable(table_name string) error {
	_, err := db.Exec("DROP TABLE " + table_name)
	return err
}

func (d *Database) CreateTableQuery(table string, columns []string) string {
	query := "CREATE TABLE " + table + " ("
	for i, column := range columns {
		query += column
		if i < len(columns)-1 {
			query += ", "
		}
	}
	query += ")"
	return query
}

func (d *Database) InsertQuery(table string, columns []string) string {
	query := "INSERT INTO " + table + " ("
	for i, column := range columns {
		query += column
		if i < len(columns)-1 {
			query += ", "
		}
	}
	query += ") VALUES ("
	for i := range columns {
		query += "?"
		if i < len(columns)-1 {
			query += ", "
		}
	}
	query += ")"
	return query
}

func (d *Database) UpdateQuery(table string, columns []string, where string) string {
	query := "UPDATE " + table + " SET "
	for i, column := range columns {
		query += column + " = ?"
		if i < len(columns)-1 {
			query += ", "
		}
	}
	query += " WHERE " + where
	return query
}

func (d *Database) DeleteQuery(table string, where string) string {
	return "DELETE FROM " + table + " WHERE " + where
}

func (d *Database) SelectQuery(table string, columns []string, where string) string {
	query := "SELECT "
	for i, column := range columns {
		query += column
		if i < len(columns)-1 {
			query += ", "
		}
	}
	query += " FROM " + table
	if where != "" {
		query += " WHERE " + where
	}
	return query
}

func (d *Database) SelectRowQuery(table string, columns []string, where string) string {
	query := "SELECT "
	for i, column := range columns {
		query += column
		if i < len(columns)-1 {
			query += ", "
		}
	}
	query += " FROM " + table
	if where != "" {
		query += " WHERE " + where
	}
	return query
}

func (d *Database) SelectOneQuery(table string, column string, where string) string {
	return d.SelectRowQuery(table, []string{column}, where)
}

func (d *Database) ExecCreateTable(table string, columns []string) error {
	_, err := d.Exec(d.CreateTableQuery(table, columns))
	return err
}

func (d *Database) ExecInsert(table string, columns []string, values []interface{}) (int64, error) {
	res, err := d.Exec(d.InsertQuery(table, columns), values...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *Database) ExecUpdate(table string, columns []string, values []interface{}, where string) error {
	_, err := d.Exec(d.UpdateQuery(table, columns, where), values...)
	return err
}

func (d *Database) ExecDelete(table string, where string) error {
	_, err := d.Exec(d.DeleteQuery(table, where))
	return err
}

func (d *Database) ExecSelect(table string, where string) (*sql.Rows, error) {
	return d.Query(d.SelectQuery(table, []string{"*"}, where))
}

func (d *Database) QuerySelect(table string, columns []string, where string) (*sql.Rows, error) {
	return d.Query(d.SelectQuery(table, columns, where))
}

func (d *Database) QuerySelectRow(table string, columns []string, where string) *sql.Row {
	return d.QueryRow(d.SelectRowQuery(table, columns, where))
}

func (d *Database) QuerySelectOne(table string, column string, where string) *sql.Row {
	return d.QueryRow(d.SelectOneQuery(table, column, where))
}
