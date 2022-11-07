package simpledb

import "strings"

// Generic filter for a queryset.
type Filter struct {
	Column   string
	Value    any
	Operator string
}

// List of filters, handy for creating a query.
type Filters []*Filter

func (f Filters) Len() int {
	return len(f)
}

// Returns a string with the query and a list of values to be used in the query.
func (f Filters) Query(and bool) (string, []interface{}) {
	var query string = ` WHERE`
	if f.Len() <= 0 {
		return "", nil
	}
	values := []any{}
	for i, v := range f {
		query += ` `
		if v.Operator == "IN" || v.Operator == "in" {
			v.Operator = strings.TrimSpace(v.Operator)
			query = query + v.Column + ` ` + v.Operator + ` (`
			for j, val := range v.Value.([]interface{}) {
				query = query + `?`
				if j < len(v.Value.([]interface{}))-1 {
					query = query + `, `
				}
				values = append(values, val)
			}
			query = query + `)`
		} else if v.Operator == "" {
			return "", nil
		} else {
			query += v.Column + ` ` + v.Operator + ` ?`
			values = append(values, v.Value)
		}
		if i < f.Len()-1 {
			if and {
				query += ` AND`
			} else {
				query += ` OR`
			}
		}
	}
	return query, values
}

// Add a filter to the list of filters.
func (f Filters) Add(column string, op string, value any) Filters {
	f = append(f, &Filter{Column: column, Value: value, Operator: strings.ToUpper(op)})
	return f
}

// Get a filter from the list of filters.
func (f Filters) Get(column string) any {
	for _, filter := range f {
		if filter.Column == column {
			return filter.Value
		}
	}
	return nil
}

// Check if a filter exists in the list of filters.
func (f Filters) Has(column string) bool {
	for _, filter := range f {
		if filter.Column == column {
			return true
		}
	}
	return false
}

// Remove a filter from the list of filters.
func (f Filters) Remove(column string) Filters {
	for i, filter := range f {
		if filter.Column == column {
			if len(f) >= i+1 {
				return append(f[:i], f[i+1:]...)
			} else {
				return f[:i]
			}
		}
	}
	return f
}
