package main

import (
	"flag"
	"database/sql"
	"fmt"
	"os"

	"github.com/cyverse-de/dbutil"
	"github.com/pkg/errors"

	_ "github.com/lib/pq"
)

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

func main() {
	var (
		permsURI = flag.String("permissions", "", "URI of the permissions database (postgresql)")
		destURI = flag.String("destination", "", "URI of the destination database (postgresql)")
		permsSchema = flag.String("permissions-schema", "permissions", "schema to use in the destination DB for the permissions tables")
	)

	flag.Parse()

	if *destURI == "" {
		fmt.Println("--destination is required")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	if *permsURI == "" {
		fmt.Println("--permissions is required")
		flag.PrintDefaults()
		os.Exit(-1)
	}

	destDB, err := initDatabase("postgres", *destURI)
	if err != nil {
		// XXX log error
		return
	}
	defer destDB.Close()

	permsDB, err := initDatabase("postgres", *permsURI)
	if err != nil {
		// XXX log error
		return
	}
	defer permsDB.Close()

	err = migratePermissions(permsDB, destDB, *permsSchema)
	if err != nil {
		// XXX log error
		return
	}
}
