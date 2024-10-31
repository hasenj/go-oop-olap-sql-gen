package sql

import "olap/set"

type Database struct {
	Name   string
	Tables set.Set[Table]
}

type Table struct {
	Name  string
	PK    string
	FKeys set.Set[ForeignKey]
}

func TableEqual(table1, table2 Table) bool {
	return table1.Name == table2.Name
}

func FKeyEqual(fk1, fk2 ForeignKey) bool {
	return fk1.Key == fk2.Key
}

type ForeignKey struct {
	Key    string
	Target string
}

func MakeDatabase(name string) Database {
	return Database{
		Name:   name,
		Tables: set.MakeSet(TableEqual),
	}
}

func MakeTable(name string, pk string) Table {
	return Table{
		Name:  name,
		PK:    pk,
		FKeys: set.MakeSet(FKeyEqual),
	}
}

func (db *Database) AddTable(t Table) bool {
	return db.Tables.Add(t)
}

func TableByName(name string) set.FnMatch[Table] {
	return func(t Table) bool {
		return t.Name == name
	}
}

func FKeyByKey(key string) set.FnMatch[ForeignKey] {
	return func(fk ForeignKey) bool {
		return fk.Key == key
	}
}

func (db *Database) GetTable(name string) Table {
	result, _ := db.Tables.Find(TableByName(name))
	return result
}

func (table *Table) AddFKey(fk ForeignKey) bool {
	return table.FKeys.Add(fk)
}

func (table *Table) GetFKey(key string) ForeignKey {
	result, _ := table.FKeys.Find(FKeyByKey(key))
	return result
}
