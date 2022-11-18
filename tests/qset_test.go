package tests

import (
	"fmt"
	"testing"

	"github.com/Nigel2392/simpledb"
)

type TestModel struct {
	ID   int64  `simpledb:"RAW:NOT NULL PRIMARY KEY AUTO_INCREMENT"`
	Name string `simpledb:"LENGTH:255"`
}

func (m *TestModel) TableName() string {
	return "test_model"
}

func TestQSetAll(t *testing.T) {
	tmodel := TestModel{}
	qs := simpledb.NewQuerySet(mDB, &tmodel).All().Where("id", simpledb.IN, []any{1, 2, 3, 4, 5, 892, 7})
	fmt.Println(qs.Query())
	models := qs.MultiModel()
	t.Log(models.String(), models.Len())
	for _, model := range models {
		model := model.(*TestModel)
		t.Log(model.ID, model.Name)
	}
}

func TestQSetGet(t *testing.T) {
	tmodel := TestModel{}
	var name string = "test1"
	err := mDB.InsertModel(&tmodel)
	if err != nil {
		t.Error(err)
	}
	// qs := simpledb.NewQuerySet(mDB, &tmodel).Get(id) //.Where("id", simpledb.EQ, 3)
	qs := simpledb.NewQuerySet(mDB, &tmodel).Get().Where("name", simpledb.EQ, name)
	model, err := qs.SingleModel()
	fmt.Println(qs.Query())
	if err != nil {
		t.Error(err)
	}
	if model == nil {
		t.Error("Expected model, got nil")
	}
	tm := model.(*TestModel)
	t.Log(tm.ID, tm.Name)
	if tm.Name != name {
		t.Error("Expected name to be", name, "got", tm.Name)
	}
}

func TestQSetPage(t *testing.T) {
	tmodel := TestModel{}
	var page int = 5
	var pagesize int = 40
	qs := simpledb.NewQuerySet(mDB, &tmodel).Limit(1000)
	qs.PAGESIZE = pagesize
	qs.All().Page(page)
	models := qs.MultiModel()
	if models.Len() != pagesize {
		t.Error("Expected ", pagesize, " got ", models.Len())
	}
	ids := make([]int64, qs.PAGESIZE)
	var i int = qs.PAGESIZE * (page - 1)
	for j := range ids {
		i++
		ids[j] = int64(i)
	}
	for i, model := range models {
		model := model.(*TestModel)
		if model.ID != ids[i] {
			t.Error("Expected ", ids[i], " got ", model.ID)
		}
	}
}

// /////////////////////////////////////////////////////////////////
//
// # BENCHMARKS
//
// /////////////////////////////////////////////////////////////////
func BenchmarkDatabaseFilter(b *testing.B) {
	var CREATE_LIMIT int = 2000
	tmodel := TestModel{}
	filter := simpledb.Filters{}
	id_list := make([]any, int(CREATE_LIMIT/4))
	for i := 1; i < int(CREATE_LIMIT/4); i++ {
		id_list[i] = i
	}
	filter = filter.Add("id", simpledb.IN, id_list)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		qs := mDB.FilterWithLimit(&tmodel, filter, int(CREATE_LIMIT/4), nil)
		_ = qs
	}
}
func BenchmarkQSetGet(b *testing.B) {
	tmodel := TestModel{}
	var name string = "test1"
	// qs := simpledb.NewQuerySet(db, &tmodel).Get(id) //.Where("id", simpledb.EQ, 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qs := simpledb.NewQuerySet(mDB, &tmodel).Get().Where("name", simpledb.EQ, name)
		_ = qs
	}
}
func BenchmarkValues(b *testing.B) {
	tmodel := TestModel{}
	models := simpledb.NewQuerySet(mDB, &tmodel).Limit(199).All().MultiModel()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		values := models.Values()
		_ = values
	}
}
