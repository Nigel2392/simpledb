package simpledb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Nigel2392/typeutils"
)

type DBType string

// Database types
const (
	VARCHAR     DBType = "VARCHAR"
	TEXT        DBType = "TEXT"
	INT         DBType = "INT"
	BOOLEAN     DBType = "BOOLEAN"
	TINYINT     DBType = "TINYINT"
	SMALLINT    DBType = "SMALLINT"
	BIGINT      DBType = "BIGINT"
	FLOAT       DBType = "FLOAT"
	DOUBLE      DBType = "DOUBLE"
	DATETIME    DBType = "DATETIME"
	BLOB        DBType = "BLOB"
	FOREIGN_KEY DBType = "FOREIGN KEY"
)

// Representation of a column,
// used to create tables when migrating
type Column struct {
	Table    string
	Name     string
	Default  string
	Type     DBType
	Raw      string
	Length   int
	Nullable bool
	Unique   bool
	Primary  bool
	Index    bool
	Auto     bool
	Tags     ModelTags
}

// Generate a query for the column
func (c Column) String() string {
	var s string
	s += c.Name + " "
	s += string(c.Type)
	if c.Raw != "" {
		s += " " + c.Raw
		return s
	}
	if c.Length > 0 {
		s += "(" + strconv.Itoa(c.Length) + ")"
	}
	if c.Nullable {
		s += " NULL"
	} else {
		s += " NOT NULL"
	}
	if c.Unique {
		s += " UNIQUE"
	}
	if c.Primary {
		s += " PRIMARY KEY"
	}
	if c.Index {
		s += " INDEX"
	}
	if c.Auto {
		s += " AUTO_INCREMENT"
	}
	if c.Default != "" {
		s += " DEFAULT " + c.Default
	}
	return s
}

// Table representation,
// used to create tables when migrating
type Table struct {
	Name      string
	Columns   []Column
	Relations []Relation
}

// Generate a query for the table
func (t Table) String() string {
	var s string
	s += "CREATE TABLE " + t.Name + " ("
	for i, c := range t.Columns {
		s += c.String()
		if i < len(t.Columns)-1 {
			s += ", "
		}
	}
	s += ")"
	return s
}

// Represents a relation between two tables.
type Relation struct {
	From string
	To   string
	Type DBType
}

// Migration represents a set of changes to the database
type Migration struct {
	Database  *Database
	Tables    []Table
	Models    []Model
	Directory string
}

// Initialize a new migration
func NewMigration(db *Database) *Migration {
	return &Migration{
		Database:  db,
		Directory: "./migrations/",
	}
}

// Create a migration from models.
func (m *Migration) CreateFromModels(models []Model) {
	for _, mdl := range models {
		table := ModelToTable(mdl)
		m.Tables = append(m.Tables, table)
	}
}

// Validate a migration
// This is used to make sure all fields are the same, if not, we can ALTER the fields on the database side.
func (m Migration) Validate(other Migration) ([]Table, []Column, []Relation, []Column, []Table, []Column, []Relation) {
	// Missing stuff
	missing_tables, removed_tables := []Table{}, []Table{}
	missing_columns, different_columns, removed_columns := []Column{}, []Column{}, []Column{}
	missing_relations, removed_relations := []Relation{}, []Relation{}
	// Check for missing tables
	for t_index, t := range m.Tables {
		if len(m.Tables) < len(other.Tables) {
			found_table := false
			for _, ot := range other.Tables {
				for _, t := range m.Tables {
					if t.Name == ot.Name {
						found_table = true
						break
					}
				}
				if !found_table {
					m.Database.Logger.Debug("MIGRATION: ", ot.Name, " was not found in the current migration, it will be removed")
					removed_tables = append(removed_tables, ot)
				}
			}
			if found_table {
				continue
			}
		}
		has_table := false
		for _, o := range other.Tables {
			if t.Name == o.Name {
				has_table = true
				break
			}
		}
		if !has_table {
			m.Database.Logger.Debug("MIGRATION: ", t.Name, " was missing, it will be added")
			missing_tables = append(missing_tables, t)
		} else {
			// Check for missing columns
			if len(t.Columns) > len(other.Tables[t_index].Columns) {
				for _, c := range t.Columns {
					has_column := false
					for _, o_c := range other.Tables[t_index].Columns {
						if c.Name == o_c.Name {
							has_column = true
							break
						}
					}
					if !has_column {
						m.Database.Logger.Debug("MIGRATION: ", c.Name, " was missing, it will be added")
						missing_columns = append(missing_columns, c)
					}
				}
			} else if len(t.Columns) < len(other.Tables[t_index].Columns) {
				for _, c := range other.Tables[t_index].Columns {
					has_column := false
					for _, o_c := range t.Columns {
						if c.Name == o_c.Name {
							has_column = true
							break
						}
					}
					if !has_column {
						m.Database.Logger.Debug("MIGRATION: ", c.Name, " was not found in the current migration, it will be removed")
						removed_columns = append(removed_columns, c)
					}
				}
			} else {
				// Check for different columns
				for _, c := range t.Columns {
					for _, o_c := range other.Tables[t_index].Columns {
						if c.Name == o_c.Name {
							if c.String() != o_c.String() {
								m.Database.Logger.Debug("MIGRATION: ", c.Name, " was different, it will be updated")
								different_columns = append(different_columns, c)
							}
							break
						}
					}
				}
			}
			// Check for missing relations
			if len(t.Relations) >= len(other.Tables[t_index].Relations) {
				for _, r := range t.Relations {
					has_relation := false
					for _, o_r := range other.Tables[t_index].Relations {
						if r.From == o_r.From && r.To == o_r.To {
							has_relation = true
							break
						}
					}
					if !has_relation {
						m.Database.Logger.Debug("MIGRATION: ", r.From, " relation was missing, it will be added")
						missing_relations = append(missing_relations, r)
					}
				}
			} else if len(t.Relations) <= len(other.Tables[t_index].Relations) {
				// Check for removed relations
				for _, r := range other.Tables[t_index].Relations {
					has_relation := false
					for _, o_r := range t.Relations {
						if r.From == o_r.From && r.To == o_r.To {
							has_relation = true
							break
						}
					}
					if !has_relation {
						m.Database.Logger.Debug("MIGRATION: ", r.From, " relation was not found in the current migration, it will be removed")
						removed_relations = append(removed_relations, r)
					}
				}
			}
		}
	}
	return missing_tables, missing_columns, missing_relations, different_columns, removed_tables, removed_columns, removed_relations
}

// Execute a migration
func (m Migration) Run() error {
	// Get the latest migration
	latest_migration, err := m.GetLatestMigration()
	if err != nil {
		return err
	}
	// Run the migrations when validating
	missing_tables, missing_columns, missing_relations, different_columns, removed_tables, removed_columns, removed_relations := m.Validate(latest_migration)
	created := []string{}
	var migrations int = 0
	if len(missing_tables) > 0 {
		// Create missing tables
		for _, t := range missing_tables {
			_, err := m.Database.Exec(t.String())
			if err != nil {
				return errors.New("failed to create table " + t.Name + ": " + err.Error())
			} else {
				created = append(created, t.Name)
			}
			migrations++
		}
		for _, t := range missing_tables {
			for _, r := range t.Relations {
				err := m.Database.CreateFKTable(r.From, r.To)
				if err != nil {
					return errors.New("error creating relation table for " + r.From + " and " + r.To + ": " + err.Error())
				}
				migrations++
			}
		}
	}
	if len(missing_columns) > 0 {
		// Add missing columns
		for _, c := range missing_columns {
			m.Database.Logger.Debug("MIGRATION: adding column ", c.Name, " to table", c.Table)
			if typeutils.Contains(created, c.Table) {
				continue
			}
			_, err := m.Database.Exec("ALTER TABLE " + c.Table + " ADD COLUMN " + c.String())
			if err != nil {
				return errors.New("error adding column " + c.Name + " to table " + c.Table + ": " + err.Error())
			}
			migrations++
		}
	}
	if len(missing_relations) > 0 {
		// Create missing relations tables
		for _, r := range missing_relations {
			m.Database.Logger.Debug("MIGRATION: creating relation table for ", r.From, " and", r.To)
			switch strings.ToLower(string(r.Type)) {
			case "fk", "foreignkey":
				err := m.Database.CreateFKTable(r.From, r.To)
				if err != nil {
					return errors.New("error creating relation table for " + r.From + " and " + r.To + ": " + err.Error())
				}
			case "1t1", "onetoone":
				err := m.Database.AlterOneToOne(r.From, r.To)
				if err != nil {
					return errors.New("error creating relation table for " + r.From + " and " + r.To + ": " + err.Error())
				}
				//case "otm", "onetomany":
				//	err := m.Database.AlterOneToMany(r.From, r.To)
				//	if err != nil {
				//		return errors.New("error creating relation table for " + r.From + " and " + r.To + ": " + err.Error())
				//	}
			}
			migrations++
		}
	}
	if len(different_columns) > 0 {
		// Update different columns
		for _, c := range different_columns {
			m.Database.Logger.Debug("MIGRATION: updating column ", c.Name, " in table", c.Table)
			_, err := m.Database.Exec("ALTER TABLE " + c.Table + " MODIFY COLUMN " + c.String())
			if err != nil {
				return errors.New("error updating column " + c.Table + "." + c.Name + ": " + err.Error())
			}
			migrations++
		}
	}
	if len(removed_tables) > 0 {
		// Remove removed tables
		for _, t := range removed_tables {
			m.Database.Logger.Debug("MIGRATION: removing table ", t.Name)
			_, err := m.Database.Exec("DROP TABLE " + t.Name)
			if err != nil {
				return errors.New("error dropping table " + t.Name + ": " + err.Error())
			}
			migrations++
		}
	}

	if len(removed_columns) > 0 {
		// Remove removed columns
		for _, c := range removed_columns {
			m.Database.Logger.Debug("MIGRATION: removing column ", c.Name, " from table ", c.Table)
			_, err := m.Database.Exec("ALTER TABLE " + c.Table + " DROP COLUMN " + c.Name)
			if err != nil {
				return errors.New("error removing column " + c.Table + "." + c.Name + ": " + err.Error())
			}
			migrations++
		}
	}

	if len(removed_relations) > 0 {
		// Remove removed relations
		for _, r := range removed_relations {
			m.Database.Logger.Debug("MIGRATION: removing relation for ", r.From, " and", r.To)
			switch strings.ToLower(string(r.Type)) {
			case "fk", "foreignkey":
				err := m.Database.DropFKTable(r.From, r.To)
				if err != nil {
					return errors.New("error dropping relation table for " + r.From + " and " + r.To + ": " + err.Error())
				}
			case "1t1", "onetoone":
				err := m.Database.AlterDropOneToOne(r.From, r.To)
				if err != nil {
					return errors.New("error dropping relation table for " + r.From + " and " + r.To + ": " + err.Error())
				}
				//case "otm", "onetomany":
				//	err := m.Database.AlterDropOneToMany(r.From, r.To)
				//	if err != nil {
				//		return errors.New("error dropping relation table for " + r.From + " and " + r.To + ": " + err.Error())
				//	}
			}
			migrations++
		}
	}

	m.Database.LatestMigration = &m

	if migrations > 0 {
		// Write migration to FS
		m.Database.Logger.Debug(fmt.Sprintf("%s migrations applied", strconv.Itoa(migrations)))
		return m.Write()
	} else {
		return errors.New("no migrations to run")
	}
}

// Read the latest migration from the file system
func (m Migration) GetLatestMigration() (Migration, error) {
	files, err := os.ReadDir(m.Directory)
	if err != nil {
		err = os.Mkdir(m.Directory, 0755)
		if err != nil {
			return Migration{}, errors.New("could not create migrations folder")
		}
	}
	// Find out when latest migration was last run
	latest_time := time.Time{}
	latest_index := 0
	for i, f := range files {
		if f.IsDir() {
			continue
		}
		if strings.HasPrefix(f.Name(), "Migration_") {
			t, err := time.Parse("2006-01-02-15-04-05", strings.TrimPrefix(strings.TrimSuffix(f.Name(), ".json"), "Migration_"))
			if err != nil {
				return Migration{}, errors.New("failed to parse migration file name: " + f.Name())
			}
			if t.After(latest_time) {
				latest_time = t
				latest_index = i
			}
		}
	}
	// Read latest migration
	latest_migration := Migration{}
	if !latest_time.IsZero() {
		if len(files) < latest_index {
			return Migration{}, errors.New("latest migration file not found")
		}
		latest_migration_file, err := os.ReadFile(m.Directory + files[latest_index].Name())
		if err != nil {
			return Migration{}, errors.New("failed to read latest migration: " + err.Error())
		}
		err = json.Unmarshal(latest_migration_file, &latest_migration)
		if err != nil {
			return Migration{}, errors.New("failed to parse latest migration: " + err.Error())
		}
	}
	return latest_migration, nil
}

// Write a migration to the file system
func (m Migration) Write() error {
	migration_file, err := json.MarshalIndent(m, "", "    ")
	if err != nil {
		return errors.New("failed to marshal migration: " + err.Error())
	}
	fname := "./migrations/Migration_" + time.Now().Format("2006-01-02-15-04-05") + ".json"
	m.Database.Logger.Debug("Writing migration to ", fname)
	err = os.WriteFile(fname, migration_file, 0644)
	if err != nil {
		return errors.New("failed to write migration file: " + err.Error())
	}
	return nil
}
