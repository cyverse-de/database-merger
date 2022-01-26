package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/cyverse-de/dbutil"
	"github.com/pkg/errors"
	gr "gonum.org/v1/gonum/graph"

	sq "github.com/Masterminds/squirrel"
	log "github.com/sirupsen/logrus"

	_ "github.com/lib/pq"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func initDatabase(driverName, databaseURI string) (*sql.DB, error) {
	wrapMsg := "unable to initialize the database"

	connector, err := dbutil.NewDefaultConnector("1m")
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	db, err := connector.Connect(driverName, databaseURI)
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	return db, nil
}

func txRollbackLogError(tx *sql.Tx) {
	err := tx.Rollback()
	if err != nil && err.Error() != "sql: transaction has already been committed or rolled back" {
		log.Warnf("Error rolling back transaction: %s", err.Error())
	}
}

func main() {
	var (
		sourceURI    = flag.String("source", "", "URI of the source database (postgresql)")
		destURI      = flag.String("destination", "", "URI of the destination database (postgresql)")
		sourceSchema = flag.String("source-schema", "public", "schema to copy into the destination database")
		destSchema   = flag.String("destination-schema", "", "schema to use in the destination DB for the tables")
	)

	flag.Parse()

	if *destURI == "" {
		fmt.Println("--destination is required")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	if *sourceURI == "" {
		fmt.Println("--source is required")
		flag.PrintDefaults()
		os.Exit(-1)
	}

	if *destSchema == "" {
		fmt.Println("--destination-schema is required")
		flag.PrintDefaults()
		os.Exit(-1)
	}

	destDB, err := initDatabase("postgres", *destURI)
	if err != nil {
		log.Fatalf("unable to connect to destination database: %s", err.Error())
	}
	defer destDB.Close()

	sourceDB, err := initDatabase("postgres", *sourceURI)
	if err != nil {
		log.Fatalf("unable to connect to source database: %s", err.Error())
	}
	defer sourceDB.Close()

	tx, err := sourceDB.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer txRollbackLogError(tx)

	_, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	if err != nil {
		log.Fatal(err)
	}

	destTx, err := destDB.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer txRollbackLogError(destTx)

	fmt.Printf("Source Schema: %s\n", *sourceSchema)
	fmt.Printf("Destination Schema: %s\n", *destSchema)

	tables, err := GetTables(tx, *sourceSchema)
	if err != nil {
		log.Fatal(err)
	}

	fks, err := GetForeignKeys(tx, tables)
	if err != nil {
		log.Fatal(err)
	}

	graph, err := MakeNodeGraph(tables, fks)
	if err != nil {
		log.Fatal(err)
	}

	ordered, err := graph.GetNodeOrder()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("TABLE ORDER")
	for _, nodeid := range ordered {
		if graph.Graph.From(nodeid) == gr.Empty {
			fmt.Printf("%s has no dependencies\n", graph.Map.Table(nodeid))
		} else {
			fromNodes := graph.Graph.From(nodeid)
			t := make([]string, fromNodes.Len())
			for i := 0; fromNodes.Next(); i++ {
				t[i] = graph.Map.Table(fromNodes.Node().ID())
			}
			fmt.Printf("%s depends on %s (%d)\n", graph.Map.Table(nodeid), strings.Join(t, ", "), len(t))
		}
		cols, err := GetTableColumns(tx, graph.Map.Table(nodeid), *sourceSchema)
		if err != nil {
			log.Fatal(err)
			return
		}
		colstrings := make([]string, len(cols))
		for i, col := range cols {
			colstrings[i] = col.String()
		}
		fmt.Printf("%s: %s\n", graph.Map.Table(nodeid), strings.Join(colstrings, ", "))
		// version table will be in the public schema in the new DB, don't try to copy it
		if graph.Map.Table(nodeid) != "version" {
			err = CopyTable(tx, destTx, graph.Map.Table(nodeid), *sourceSchema, *destSchema, true)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	err = destTx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Committed transaction")
}
