package tests

import (
	"strings"
	"testing"

	"github.com/Nigel2392/simpledb"
)

type ModelOne struct {
	Id            int64     `simpledb:"RAW:NOT NULL PRIMARY KEY AUTO_INCREMENT"`
	Name          string    `simpledb:"LENGTH:255"`
	Rel_model_two *ModelTwo `simpledb:"RELTYPE:FK"` // Related model fields always start with Rel_
}

type ModelTwo struct {
	Id   int64  `simpledb:"RAW:NOT NULL PRIMARY KEY AUTO_INCREMENT"`
	Name string `simpledb:"LENGTH:255"`
}

type ModelThree struct {
	Id   int64  `simpledb:"RAW:NOT NULL PRIMARY KEY AUTO_INCREMENT"`
	Name string `simpledb:"LENGTH:255,UNIQUE:true"`
}

func (m *ModelOne) TableName() string {
	return "model_one"
}

func (m *ModelTwo) TableName() string {
	return "model_two"
}

func (m *ModelThree) TableName() string {
	return "model_three"
}

var mDB = simpledb.NewDatabase()

func init() {
	mDB.Host = "localhost"
	mDB.Port = 3306
	mDB.Username = "testing_http"
	mDB.Password = "TESTING_HTTP"
	mDB.Database = "testing_http"
	mDB.SSL_MODE = "disable"
	mDB.Register(&ModelOne{})
	mDB.Register(&ModelTwo{})
	mDB.Register(&ModelThree{})
	mDB.Register(&TestModel{})
	mDB.Connect()
}

func TestMigration(t *testing.T) {
	err := mDB.Migrate()
	if err != nil {
		if strings.EqualFold(err.Error(), "no migrations to run") {
			t.Log("No migrations to run")
		} else {
			t.Error(err)
		}
	}
}

func TestRelated(t *testing.T) {
	one := &ModelOne{
		Name: "One",
	}
	two := &ModelTwo{
		Name: "Two",
	}
	err := mDB.InsertModel(one)
	if err != nil {
		t.Error(err)
	}
	err = mDB.InsertModel(two)
	if err != nil {
		t.Error(err)
	}

	t.Log("One:", one)
	t.Log("Two:", two)

	err = mDB.InsertFK(one, two)
	if err != nil {
		t.Error(err)
	}

	two.Name = "TwoTwo"
	two.Id = 0

	err = mDB.InsertModel(two)
	if err != nil {
		t.Error(err)
	}

	t.Log("TwoTwo:", two)

	err = mDB.InsertFK(one, two)
	if err != nil {
		t.Error(err)
	}

	models, err := mDB.SelectFK(one, two)
	if err != nil {
		t.Error(err)
	}
	if len(models) == 0 {
		t.Error("No models found")
	}
	for _, model := range models {
		t.Log("\tFOREIGN KEY: ", model.(*ModelTwo))
	}
}
