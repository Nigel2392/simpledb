package simpledb

import (
	"reflect"
	"strconv"
	"strings"
)

const TAG = "simpledb"

type ModelTags map[string]string

func (t ModelTags) Get(key string) string {
	return t[key]
}

func (t ModelTags) Set(key string, value string) {
	t[key] = value
}

func (t ModelTags) Has(key string) bool {
	_, ok := t[key]
	return ok
}

func (t ModelTags) Length() int {
	n, err := strconv.Atoi(t.Get("LENGTH"))
	if err != nil {
		return 0
	}
	return n
}
func (t ModelTags) Nullable() bool {
	v := t.Get("NULLABLE")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}
func (t ModelTags) Unique() bool {
	v := t.Get("UNIQUE")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}
func (t ModelTags) Primary() bool {
	v := t.Get("PRIMARY")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}
func (t ModelTags) Index() bool {
	v := t.Get("INDEX")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}
func (t ModelTags) Auto() bool {
	v := t.Get("AUTO")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}
func (t ModelTags) Default() string {
	return t.Get("DEFAULT")
}

func (t ModelTags) RelType() string {
	return t.Get("RELTYPE")
}

func (t ModelTags) Raw() string {
	return t.Get("RAW")
}

func (t ModelTags) Type() string {
	return t.Get("TYPE")
}

func TagValid(field reflect.StructField) bool {
	tag := field.Tag.Get(TAG)
	if tag == "-" {
		return false
	} else if tag == "" {
		return false
	}
	return true
}

func TagValues(field reflect.StructField) []string {
	tag := field.Tag.Get(TAG)
	return strings.Split(tag, ",")
}

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
