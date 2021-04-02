package main

import (
	"database/sql"

	"github.com/pkg/errors"

	sq "github.com/Masterminds/squirrel"
)

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
