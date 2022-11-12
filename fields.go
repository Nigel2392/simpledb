package simpledb

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"

	"github.com/Nigel2392/typeutils"
)

// Get the column types for a specified golang type
func GetColType(typ string, dialect string) string {
	switch strings.ToLower(dialect) {
	case "sqlite", "sqlite3":
		switch typ {
		case "string":
			return "TEXT"
		case "int":
			return "INTEGER"
		case "bool":
			return "BOOLEAN"
		case "int8":
			return "INTEGER"
		case "int16":
			return "INTEGER"
		case "int32":
			return "INTEGER"
		case "int64":
			return "INTEGER"
		case "float32":
			return "FLOAT"
		case "float64":
			return "FLOAT"
		case "Time":
			return "DATETIME"
		case "[]byte":
			return "BLOB"
		default:
			return "TEXT"
		}
	case "mysql", "mariadb", "mssql", "sqlserver":
		switch typ {
		case "string":
			return "VARCHAR"
		case "int":
			return "INT"
		case "bool":
			return "BOOLEAN"
		case "int8":
			return "TINYINT"
		case "int16":
			return "SMALLINT"
		case "int32":
			return "INT"
		case "int64":
			return "BIGINT"
		case "float32":
			return "FLOAT"
		case "float64":
			return "DOUBLE"
		case "Time":
			return "DATETIME"
		case "[]byte":
			return "BLOB"
		default:
			return "VARCHAR"
		}
	}
	return ""
}

// Get the column types for the model
func GetColTypes(types []string, dialect string) []string {
	for i, typ := range types {
		types[i] = GetColType(typ, dialect)
	}
	return types
}

// Get the model columns
func Columns(model any) []string {
	// Validate kind
	kind := modelKind(model)
	// Loop through all fields in the struct
	columns := []string{}
	inlineLoopFields(kind, func(f reflect.StructField, i int) {
		if isRelated(f) {
			return
		}
		// Get the name of the struct field
		columns = append(columns, strings.ToLower(f.Name))
	})
	return columns
}

// Get the columns needed for a migration
func MigrationColumns(model Model) []Column {
	kind := modelKind(model)
	columns := []Column{}
	inlineLoopFields(kind, func(f reflect.StructField, i int) {
		if isRelated(f) {
			return //Skip related fields
		}
		var tv ModelTags = TagMap(f)
		typ := GetColType(f.Type.Name(), "mysql")
		col := tv.ToColumn(model.TableName(), strings.ToLower(f.Name), typ)
		columns = append(columns, col)
	})
	return columns
}

// Get the related fields for migrating a model.
func MigrationRelations(model Model) []Relation {
	kind := modelKind(model)
	relations := []Relation{}
	for i := 0; i < kind.NumField(); i++ {
		f := kind.Field(i)

		if f.Tag.Get(TAG) == "" {
			continue
		} else if f.Tag.Get(TAG) == "-" {
			continue
		}
		if isRelated(f) {
			tm := TagMap(f)
			other := strings.TrimPrefix(f.Name, "Rel_")
			relations = append(relations, Relation{
				From: model.TableName(),
				To:   other, //Provide the name of the other table
				Type: DBType(tm.RelType()),
			})
		}
	}
	return relations
}

// Get the columns with golang types
func ColumnsWithTypes(model any) ([]string, []string) {
	// Validate kind
	kind := modelKind(model)
	// Columns to return
	types := make([]string, 0)
	columns := []string{}
	inlineLoopFields(kind, func(f reflect.StructField, i int) {
		if isRelated(f) {
			return //Skip related fields
		}
		// Get the name of the struct field
		columns = append(columns, strings.ToLower(f.Name))
		// Get the type of the struct field
		types = append(types, f.Type.Name())
	})
	return columns, types
}

// Get a value from a model struct
func GetValue(model Model, column string) any {
	// Validate kind
	kind := modelKind(model)
	// Loop through all fields in the struct
	for i := 0; i < kind.NumField(); i++ {
		f_kind := kind.Field(i)
		if !TagValid(f_kind) {
			continue
		}
		// Get the name of the struct field
		if strings.EqualFold(f_kind.Name, column) {
			var val any
			// var err error
			if isRelated(f_kind) {
				// TODO: Handle related fields
				// Query the related table
				// Get the value of the related fields
				continue
			}
			if f_kind.Type.Kind() == reflect.Ptr {
				val = reflect.ValueOf(model).Elem().Field(i).Elem().Interface()
			} else {
				val = reflect.ValueOf(model).Elem().Field(i).Interface()
			}
			return val
		}
	}
	return nil
}

// Set a value on a model struct
func SetValue(model Model, column string, value any) {
	// Validate kind
	kind := modelKind(model)
	// Loop through all fields in the struct
	for i := 0; i < kind.NumField(); i++ {
		f_kind := kind.Field(i)
		if !TagValid(f_kind) {
			continue
		}
		if isRelated(f_kind) {
			continue
		}
		// Get the name of the struct field
		if strings.EqualFold(f_kind.Name, column) {
			// Set the value of the struct field
			// Check if types match
			reflect.ValueOf(model).Elem().Field(i).Set(reflect.ValueOf(value))
			return
		}
	}
}

// Validate if field is a related field
func isRelated(f reflect.StructField) bool {
	return strings.HasPrefix(strings.ToLower(f.Name), "rel_")
}

// Scan a row into a model
func Scan(model Model, row *sql.Rows, exclude []string) error {
	// Validate kind
	fields, err := modelFields(model, exclude)
	if err != nil {
		return err
	}
	return row.Scan(fields...)
}

// Get the model fields to scan into
func modelFields(model Model, include []string) ([]any, error) {
	if len(include) == 0 {
		include = Columns(model)
	}
	// Use reflection to get the columns from the model
	typeof := reflect.TypeOf(model)
	struct_fields := make([]any, 0)
	if typeof.Kind() != reflect.Ptr {
		return nil, errors.New("model is not a pointer to struct")
	}
	typeof = typeof.Elem()
	for i := 0; i < typeof.NumField(); i++ {
		if !TagValid(typeof.Field(i)) {
			continue
		} else if isRelated(typeof.Field(i)) {
			continue
		}
		if typeutils.Contains(include, strings.ToLower(typeof.Field(i).Name)) {
			struct_fields = append(struct_fields, reflect.ValueOf(model).Elem().Field(i).Addr().Interface())
		}
	}
	return struct_fields, nil
}
