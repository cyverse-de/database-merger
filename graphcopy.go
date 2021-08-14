package main

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/pkg/errors"

	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"
)

type ForeignKey struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
}

type Column struct {
	ColumnName string
	DataType   string
}

func (c *Column) String() string {
	return fmt.Sprintf("%s (%s)", c.ColumnName, c.DataType)
}

type TableNodeMap struct {
	ntMap map[int64]string
	tnMap map[string]int64
}

func (t *TableNodeMap) Table(nodeid int64) string {
	return t.ntMap[nodeid]
}

func (t *TableNodeMap) Node(table string) int64 {
	return t.tnMap[table]
}

type TableGraph struct {
	Graph *simple.DirectedGraph
	Map   *TableNodeMap
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

func GetTableColumns(tx *sql.Tx, table string, schema string) ([]Column, error) {
	rows, err := psql.
		Select().
		Columns("column_name, data_type").
		From("information_schema.columns").
		Where(sq.And{
			sq.Eq{"table_name": table},
			sq.Eq{"table_schema": schema},
		}).
		RunWith(tx).Query()
	if err != nil {
		return nil, errors.Wrap(err, "GetTableColumns: error running columns query")
	}
	defer rows.Close()

	var cols []Column
	for rows.Next() {
		var col Column
		err = rows.Scan(&col.ColumnName, &col.DataType)
		if err != nil {
			return nil, errors.Wrap(err, "GetTableColumns: error scanning row")
		}
		cols = append(cols, col)
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "GetTableColumns: rows.Err()")
	}
	return cols, err
}

func RunInsert(tx *sql.Tx, builder sq.InsertBuilder, priorRows int64) error {
	res, err := builder.RunWith(tx).Exec()
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		fmt.Printf("RunInsert: Error getting affected rows: %s\n", err.Error())
	}
	fmt.Printf("Inserted %d rows (%d new)\n", rows+priorRows, rows)
	return nil
}

func CopyTable(sourceTx *sql.Tx, destTx *sql.Tx, table string, sourceSchema string, destSchema string, deleteFirst bool) error {
	var batchSize int64 = 10000 // maybe pass as argument or depend on number of columns? this allows 6 columns (65535 max args, 6 columns * 10000 rows)
	cols, err := GetTableColumns(sourceTx, table, sourceSchema)
	if err != nil {
		return errors.Wrap(err, "CopyTable: error getting table columns")
	}
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.ColumnName
	}
	// check if dest matches?

	sourceData, err := psql.
		Select().
		Columns(colNames...).
		From(fmt.Sprintf("%s.%s", sourceSchema, table)).
		RunWith(sourceTx).Query()
	if err != nil {
		return errors.Wrap(err, "CopyTable: error fetching source data")
	}

	if deleteFirst {
		res, err := psql.Delete(fmt.Sprintf("%s.%s", destSchema, table)).RunWith(destTx).Exec()
		if err != nil {
			return errors.Wrap(err, "CopyTable: deleting old data")
		}
		rows, err := res.RowsAffected()
		if err != nil {
			fmt.Printf("CopyTable: Error getting affected rows after delete: %s\n", err.Error())
		}
		fmt.Printf("Deleted %d rows from %s.%s\n", rows, destSchema, table)
	}

	builder := psql.
		Insert(fmt.Sprintf("%s.%s", destSchema, table)).
		Columns(colNames...)

	var toInsertRows, insertedRows int64
	toInsertRows = 0
	insertedRows = 0
	for sourceData.Next() {
		data := make([]interface{}, len(cols))
		dp := make([]interface{}, len(cols))
		for i := range data {
			dp[i] = &data[i]
		}

		err = sourceData.Scan(dp...)
		if err != nil {
			return errors.Wrap(err, "CopyTable: error scanning row")
		}

		builder = builder.Values(data...)
		toInsertRows++

		if toInsertRows >= batchSize {
			err = RunInsert(destTx, builder, insertedRows)
			if err != nil {
				return errors.Wrap(err, "CopyTable: inserting rows")
			}
			insertedRows = insertedRows + toInsertRows
			// refresh number of rows and builder
			toInsertRows = 0
			builder = psql.
				Insert(fmt.Sprintf("%s.%s", destSchema, table)).
				Columns(colNames...)
		}
	}

	// clear out any remaining rows
	if toInsertRows > 0 {
		err = RunInsert(destTx, builder, insertedRows)
		if err != nil {
			return errors.Wrap(err, "CopyTable: inserting rows")
		}
	}

	return nil
}

func MakeNodeGraph(tables []string, fks []ForeignKey) (*TableGraph, error) {

	graph := simple.NewDirectedGraph()
	nodemap := make(map[string]int64)
	backmap := make(map[int64]string)
	for _, table := range tables {
		node := graph.NewNode()
		graph.AddNode(node)
		nodemap[table] = node.ID()
		backmap[node.ID()] = table
	}

	for _, fk := range fks {
		fromId := nodemap[fk.FromTable]
		toId := nodemap[fk.ToTable]
		if !graph.HasEdgeFromTo(fromId, toId) && (graph.Node(fromId) != nil) && (graph.Node(toId) != nil) {
			graph.SetEdge(graph.NewEdge(graph.Node(fromId), graph.Node(toId)))
		} else if graph.Node(fromId) == nil || graph.Node(toId) == nil {
			return nil, errors.New("A table referenced in an FK is not in the graph")
		}
	}

	return &TableGraph{Graph: graph, Map: &TableNodeMap{tnMap: nodemap, ntMap: backmap}}, nil
}

func (t *TableGraph) GetNodeOrder() ([]int64, error) {
	sorted, err := topo.Sort(t.Graph)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't sort the graph. This probably means that the set of tables is not a directed acyclic graph, and that probably means we can't help you here.")
	}
	// Reverse the array so it's the order to copy tables, not the opposite
	for i, j := 0, len(sorted)-1; i < j; i, j = i+1, j-1 {
		sorted[i], sorted[j] = sorted[j], sorted[i]
	}

	ret := make([]int64, len(sorted))
	// Replace Nodes with their IDs
	for i := range sorted {
		ret[i] = sorted[i].ID()
	}

	return ret, nil
}
