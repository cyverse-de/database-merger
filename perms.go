package main

import (
	"database/sql"

	"github.com/pkg/errors"

	sq "github.com/Masterminds/squirrel"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func migrateSubjects(sourceTx, destTx *sql.Tx, schema string) error {
	wrapMsg := "subject migration failed"

	// Do we need to check if the table is empty?
	sourceRows, err := psql.Select("id, subject_id, subject_type").From("subjects").RunWith(sourceTx).Query()
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}
	defer sourceRows.Close()

	builder := psql.Insert(schema+".subjects").
		Columns("id", "subject_id", "subject_type").
		Suffix("ON CONFLICT (id) DO UPDATE SET subject_id=EXCLUDED.subject_id, subject_type=EXCLUDED.subject_type")

	for sourceRows.Next() {
		var id, subjectId, subjectType string
		err = sourceRows.Scan(&id, &subjectId, &subjectType)
		if err != nil {
			return errors.Wrap(err, wrapMsg)
		}
		builder = builder.Values(id, subjectId, subjectType)
	}

	_, err = builder.RunWith(destTx).Exec()
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	return nil
}

func migratePermissions(permsDB *sql.DB, destDB *sql.DB, schema string) error {
	permsTx, err := permsDB.Begin()
	if err != nil {
		// XXX log error
		return err
	}
	defer permsTx.Rollback()

	destTx, err := destDB.Begin()
	if err != nil {
		// XXX log error
		return err
	}
	defer destTx.Rollback()

	// Lock everything so only concurrent reads can happen, just in case
	_, err = permsTx.Exec("LOCK TABLE subjects, resource_types, resources, permission_levels, permissions IN EXCLUSIVE MODE")
	if err != nil {
		// XXX log error
		return err
	}

	err = migrateSubjects(permsTx, destTx, schema)
	if err != nil {
		// XXX log error
		return err
	}

	return destTx.Commit()
}
