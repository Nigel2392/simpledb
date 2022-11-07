package simpledb

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// ModelSet is a client-side representation of a queryset.
// Allows limited filtering.
type ModelSet []Model

// Len returns the number of models in the ModelSet.
func (ms ModelSet) Len() int {
	return len(ms)
}

// String returns a string representation of the ModelSet.
func (ms ModelSet) String() string {
	var str string = "ModelSet: " + strconv.Itoa(ms.Len()) + " models"
	return str
}

// Filter a modelset, returns a new modelset.
// Example:
//
//	qs.Filter("name=John,Paul")
//	qs.Filter("age=18,19,20")
//
// Better to use on smaller sets, less than 1000.
// Otherwise use the Database.Filter() method.
func (ms ModelSet) Filter(args []string) ModelSet {
	newqs := ModelSet{}
	for _, arg := range args {
		kv := strings.SplitN(arg, "=", 2)
		key := kv[0]
		values := strings.Split(kv[1], ",")
		wg := sync.WaitGroup{}
		mut := sync.Mutex{}
		guard := make(chan struct{}, 40)
		wg.Add(len(ms))
		for _, m := range ms {
			guard <- struct{}{}
			go func(m Model, wg *sync.WaitGroup, mut *sync.Mutex) {
				defer wg.Done()
				cols := Columns(m)
				for _, col := range cols {
					if strings.EqualFold(col, key) {
						for _, value := range values {
							col_value := GetValue(m, col)
							if fmt.Sprint(col_value) == value {
								mut.Lock()
								newqs = append(newqs, m)
								mut.Unlock()
							}
						}
					}
				}
				<-guard
			}(m, &wg, &mut)
		}
		wg.Wait()
	}
	ms = newqs
	return ms
}

// Return a slice of slices of values.
func (ms ModelSet) Values(exclude ...string) []map[string]any {
	var newvals []map[string]any = []map[string]any{}
	if ms.Len() <= 0 {
		panic("ModelSet is empty")
	}
	cols := Columns(ms[0])
	cols = Exclude(cols, exclude)
	var g_len int
	if ms.Len() <= 200 {
		g_len = int(ms.Len() / 4)
	} else {
		g_len = int(40)
	}
	guard, wg, mu := initSync(g_len)
	wg.Add(ms.Len())
	for _, m := range ms {
		guard <- struct{}{}
		go func(m Model, wg *sync.WaitGroup, mu *sync.Mutex) {
			defer wg.Done()
			vals := make(map[string]any)
			for _, col := range cols {
				vals[col] = GetValue(m, col)
			}
			mu.Lock()
			newvals = append(newvals, vals)
			mu.Unlock()
			<-guard
		}(m, wg, mu)
	}
	wg.Wait()
	return newvals
}
