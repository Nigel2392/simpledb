package simpledb

import (
	"reflect"
	"strconv"
	"strings"
)

// Tag to get from model structs.
const TAG = "simpledb"

// ModelTags is a map of tags for a model.
type ModelTags map[string]string

// Get a tag value.
func (t ModelTags) Get(key string) string {
	return t[key]
}

// Set a tag value.
func (t ModelTags) Set(key string, value string) {
	t[key] = value
}

// Check if tag exists.
func (t ModelTags) Has(key string) bool {
	_, ok := t[key]
	return ok
}

// Get the length of a column.
func (t ModelTags) Length() int {
	n, err := strconv.Atoi(t.Get("LENGTH"))
	if err != nil {
		return 0
	}
	return n
}

// Verify if column is nullable.
func (t ModelTags) Nullable() bool {
	v := t.Get("NULLABLE")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

// Verify if column is unique.
func (t ModelTags) Unique() bool {
	v := t.Get("UNIQUE")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

// Verify if column is a primary key.
func (t ModelTags) Primary() bool {
	v := t.Get("PRIMARY")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

// Verify if column needs an index.
func (t ModelTags) Index() bool {
	v := t.Get("INDEX")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

// Auto is a special tag that indicates that the column is auto-incrementing.
func (t ModelTags) Auto() bool {
	v := t.Get("AUTO")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

// Default is a special tag that indicates that the column has a default value.
// This default value is however very limited, and can only be set in the
// model tags.
func (t ModelTags) Default() string {
	return t.Get("DEFAULT")
}

// Get the relation type from the tag.
func (t ModelTags) RelType() string {
	return t.Get("RELTYPE")
}

// Used for annotating your type tags with raw sql.
// Example:
//
//	type User struct {
//	    ID int `simpledb:"RAW: NOT NULL AUTO_INCREMENT PRIMARY"`
//	}
func (t ModelTags) Raw() string {
	return t.Get("RAW")
}

// Get the column type from the tag.
func (t ModelTags) Type() string {
	return t.Get("TYPE")
}

// Verify if a field/tag is valid to be used.
func TagValid(field reflect.StructField) bool {
	tag := field.Tag.Get(TAG)
	if tag == "-" {
		return false
	} else if tag == "" {
		return false
	}
	return true
}

// Split tags into lists of [Key:Value, Key:Value, ...] pairs.
func TagValues(field reflect.StructField) []string {
	tag := field.Tag.Get(TAG)
	return strings.Split(tag, ",")
}

// Generate a map from the list of [Key:Value, Key:Value, ...] pairs.
func TagMap(field reflect.StructField) ModelTags {
	tag := field.Tag.Get(TAG)
	tags := strings.Split(tag, ",")
	tagmap := make(map[string]string)
	for _, v := range tags {
		if v == "+" {
			continue
		}
		tag := strings.SplitN(v, ":", 2)
		if len(tag) != 2 {
			panic("invalid tag")
		}
		tagmap[tag[0]] = tag[1]
	}
	return tagmap
}

// Convert tag map to a column used for migrations.
func (t ModelTags) ToColumn(tname, name, typ string) Column {
	if t.Type() != "" {
		typ = t.Type()
	}
	return Column{
		Table:    tname,
		Name:     name,
		Type:     DBType(typ),
		Length:   t.Length(),
		Nullable: t.Nullable(),
		Unique:   t.Unique(),
		Primary:  t.Primary(),
		Index:    t.Index(),
		Auto:     t.Auto(),
		Default:  t.Default(),
		Raw:      t.Raw(),
		Tags:     t,
	}
}
