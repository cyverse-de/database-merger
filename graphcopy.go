package main

import (
	"database/sql"

	"github.com/pkg/errors"

	sq "github.com/Masterminds/squirrel"
)

type ForeignKey struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
}

func GetTables(tx *sql.Tx, schema string) ([]string, error) {
	rows, err := psql.
		Select("table_name").
		From("information_schema.tables").
		Where(sq.Eq{"table_schema": schema}).
		RunWith(tx).Query()
	if err != nil {
		return nil, errors.Wrap(err, "GetTables: error running table_name query")
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		err = rows.Scan(&t)
		if err != nil {
			return nil, errors.Wrap(err, "GetTables: error scanning row")
		}
		tables = append(tables, t)
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "GetTables: rows.Err()")
	}
	return tables, err
}

func GetForeignKeys(tx *sql.Tx, tables []string) ([]ForeignKey, error) {
	rows, err := psql.
		Select().
		Columns("tc.table_name, kcu.column_name, ccu.table_name, ccu.column_name").
		From("information_schema.table_constraints AS tc").
		Join("information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema").
		Join("information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema").
		Where(sq.And{
			sq.Eq{"tc.constraint_type": "FOREIGN KEY"},
			sq.Eq{"tc.table_name": tables},
		}).
		RunWith(tx).Query()
	if err != nil {
		return nil, errors.Wrap(err, "GetForeignKeys: error running foreign key query")
	}
	defer rows.Close()

	var fks []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		err = rows.Scan(&fk.FromTable, &fk.FromColumn, &fk.ToTable, &fk.ToColumn)
		if err != nil {
			return nil, errors.Wrap(err, "GetForeignKeys: error scanning row")
		}
		fks = append(fks, fk)
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "GetForeignKeys: rows.Err()")
	}
	return fks, err

}
