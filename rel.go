package simpledb

func (db *Database) CreateFKTable(from, to string) error {
	query := `CREATE TABLE IF NOT EXISTS ` + from + `_` + to + ` (
		id BIGINT PRIMARY KEY AUTO_INCREMENT,
		` + from + `_id BIGINT,
		` + to + `_id BIGINT,
		FOREIGN KEY (` + from + `_id) REFERENCES ` + from + `(id),
		FOREIGN KEY (` + to + `_id) REFERENCES ` + to + `(id)
	)`
	_, err := db.Exec(query)
	return err
}

func (db *Database) InsertFK(from, to Model) error {
	query := `INSERT INTO ` + from.TableName() + `_` + to.TableName() + ` (` + from.TableName() + `_id, ` + to.TableName() + `_id) VALUES (?, ?)`
	_, err := db.Exec(query, GetValue(from, "id"), GetValue(to, "id"))
	return err
}

func (db *Database) DeleteFK(from, to Model) error {
	query := `DELETE FROM ` + from.TableName() + `_` + to.TableName() + ` WHERE ` + from.TableName() + `_id = ? AND ` + to.TableName() + `_id = ?`
	_, err := db.Exec(query, GetValue(from, "id"), GetValue(to, "id"))
	return err
}

func (db *Database) DropFKTable(from, to string) error {
	query := `DROP TABLE IF EXISTS ` + from + `_` + to
	_, err := db.Exec(query)
	return err
}

func (db *Database) SelectFK(from, to Model) (ModelSet, error) {
	query := `SELECT * FROM ` + to.TableName() + ` WHERE id IN (SELECT ` + to.TableName() + `_id FROM ` + from.TableName() + `_` + to.TableName() + ` WHERE ` + from.TableName() + `_id = ?)`
	rows, err := db.Query(query, GetValue(from, "id"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	qs := ScanRows(rows, to)
	return qs, nil
}

func (db *Database) SelectFKReverse(from, to Model) (ModelSet, error) {
	query := `SELECT * FROM ` + from.TableName() + ` WHERE id IN (SELECT ` + from.TableName() + `_id FROM ` + from.TableName() + `_` + to.TableName() + ` WHERE ` + to.TableName() + `_id = ?)`
	rows, err := db.Query(query, GetValue(to, "id"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	qs := ScanRows(rows, from)
	return qs, nil
}

func (db *Database) AlterOneToOne(from, to string) error {
	query := `ALTER TABLE ` + from + ` ADD CONSTRAINT fk_` + to + ` FOREIGN KEY (` + to + `_id) references ` + from + `(id) `
	_, err := db.Exec(query)
	return err
}

func (db *Database) AlterOneToMany(from, to string) error {
	query := `ALTER TABLE ` + to + ` ADD CONSTRAINT fk_` + from + ` FOREIGN KEY (` + from + `_id) references ` + from + `(id) `
	_, err := db.Exec(query)
	return err
}

func (db *Database) GetOneToOne(from, to Model) (ModelSet, error) {
	query := `SELECT * FROM ` + from.TableName() + ` WHERE ` + to.TableName() + `_id IS NOT NULL`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	qs := ScanRows(rows, from)
	return qs, nil
}

func (db *Database) GetOneToMany(from, to Model) (ModelSet, error) {
	query := `SELECT * FROM ` + from.TableName() + ` WHERE ` + to.TableName() + `_id IS NULL`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	qs := ScanRows(rows, from)
	return qs, nil
}

func (db *Database) AlterDropOneToOne(from, to string) error {
	query := `ALTER TABLE ` + from + ` DROP FOREIGN KEY fk_` + to
	_, err := db.Exec(query)
	return err
}

func (db *Database) AlterDropOneToMany(from, to string) error {
	query := `ALTER TABLE ` + to + ` DROP FOREIGN KEY fk_` + from
	_, err := db.Exec(query)
	return err
}
