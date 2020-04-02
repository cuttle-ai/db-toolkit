// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package postgres has the datastore implementation for the postgres database to store data in cuttle platform
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	//this package contains the postgres driver for cuttle to use it as a datastore. That is why the initalization done here
	_ "github.com/lib/pq"

	"github.com/cuttle-ai/brain/log"
	"github.com/cuttle-ai/octopus/interpreter"
)

//Postgres is the postgre datastore
type Postgres struct {
	//DB connection instance
	DB *sql.DB
}

//NewPostgres returns the postgres with active connection
func NewPostgres(host, port, dbName, username, password string) (*Postgres, error) {
	cStr := fmt.Sprintf("host=%s port=%s dbname=%s  user=%s password=%s sslmode=disable",
		host, port, dbName, username, password)
	db, err := sql.Open("postgres", cStr)
	if err != nil {
		return nil, err
	}
	return &Postgres{DB: db}, nil
}

func convertToPostgresDataType(dataType string) string {
	switch dataType {
	case interpreter.DataTypeString:
		return "text"
	case interpreter.DataTypeFloat:
		return "float"
	case interpreter.DataTypeDate:
		return "date"
	default:
		return "text"
	}
}

//DumpCSV will dump the given csv file to post instance
func (p Postgres) DumpCSV(filename string, tablename string, columns []interpreter.ColumnNode, appendData bool, createTable bool, logger log.Log) error {
	/*
	 * First we will start a transaction for the db operation
	 * Then we will create the table required
	 * Then we will dump the data to the datastore
	 */
	//starting the db transaction
	tx, err := p.DB.BeginTx(context.Background(), nil)
	if err != nil {
		logger.Error("error while creating the db transaction for dumping csv to the datastore")
		return err
	}

	//we will create the table
	//we will first build the query string to create the table
	logger.Info("building the table to dump the csv", tablename)
	var strB strings.Builder
	var strC strings.Builder
	strB.WriteString("CREATE TABLE ")
	strB.WriteString(tablename)
	strB.WriteString("( ")
	strC.WriteString("(")
	for k, col := range columns {
		if k > 0 {
			strB.WriteString(", ")
			strC.WriteString(", ")
		}
		strB.WriteString(col.Name + " " + convertToPostgresDataType(col.DataType))
		strC.WriteString(col.Name)
	}
	strB.WriteString(" )")
	strC.WriteString(" )")
	//now executing the built query

	if createTable {
		_, err = tx.Exec(strB.String())
		if err != nil {
			logger.Error("error while creating the table", tablename, "for dumping the csv data to the datastore")
			return err
		}
	}

	//now we will dump the data to the datastore
	logger.Info("copying the data from the csv to the table", filename, tablename)
	qStr := fmt.Sprintf(`COPY %s %s FROM '%s' DELIMITER ',' CSV HEADER;`, tablename, strC.String(), filename)
	result, err := tx.Exec(qStr)
	if err != nil {
		logger.Error("error while dumping to the table", tablename, "from csv", filename)
		return err
	}
	ef, err := result.RowsAffected()
	if err != nil {
		logger.Error("error while getting the number of rows affected while dumping the data to the datastore")
		return err
	}

	err = tx.Commit()
	if err != nil {
		logger.Error("error while commiting the changes")
		return err
	}

	logger.Info("successfully dumped the csv to the table", filename, tablename, "copied no. of rows:-", ef)
	return nil
}

//DeleteTable deletes the table from the datastore
func (p Postgres) DeleteTable(tablename string) error {
	_, err := p.DB.Exec("drop table ?", tablename)
	return err
}
