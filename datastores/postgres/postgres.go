// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package postgres has the datastore implementation for the postgres database to store data in cuttle platform
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
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
	//DataDumpDirectory will be the name of the directory with the user name attached to it
	//Eg. user@myserver.com:/home/user/data-directory
	DataDumpDirectory string
}

//NewPostgres returns the postgres with active connection
func NewPostgres(host, port, dbName, username, password, dataDumpDirectory string) (*Postgres, error) {
	cStr := fmt.Sprintf("host=%s port=%s dbname=%s  user=%s password=%s sslmode=disable",
		host, port, dbName, username, password)
	db, err := sql.Open("postgres", cStr)
	if err != nil {
		return nil, err
	}
	return &Postgres{DB: db, DataDumpDirectory: dataDumpDirectory}, nil
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
	 * We will copy the file to the remote
	 * We will start a transaction for the db operation
	 * Then we will create the table required
	 * Then we will dump the data to the datastore
	 * Then we will remove the file from the remote
	 */
	//copying the file to the remote
	logger.Info("copying the file to remote postgres server", p.DataDumpDirectory)
	cm := exec.Command("scp", filename, p.DataDumpDirectory)
	err := cm.Run()
	if err != nil {
		logger.Error("error copying the file for dumping csv to the datastore", filename, "to", p.DataDumpDirectory)
		return err
	}

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
	fileNameSplitted := strings.Split(filename, string([]rune{filepath.Separator}))
	if len(fileNameSplitted) < 1 {
		logger.Error("expected the filename to have atleast 1 part. got", len(fileNameSplitted))
		return errors.New("expected the filename to have atleast 1 part. got " + strconv.Itoa(len(fileNameSplitted)))
	}
	remoteFileName := p.DataDumpDirectory + "/" + fileNameSplitted[len(fileNameSplitted)-1]
	remoteFileNameSplitted := strings.Split(remoteFileName, ":")
	if len(remoteFileNameSplitted) < 2 {
		logger.Error("expected the filename to have server info like user@server.com:/home/user. Couldn't find one", remoteFileName)
		return errors.New("expected the filename to have server info like user@server.com:/home/user. Couldn't find one + remoteFileName")
	}
	remoteFileNameWithoutServer := remoteFileNameSplitted[len(remoteFileNameSplitted)-1]

	logger.Info("copying the data from the csv to the table", remoteFileNameWithoutServer, tablename)
	qStr := fmt.Sprintf(`COPY %s %s FROM '%s' DELIMITER ',' CSV HEADER;`, tablename, strC.String(), remoteFileNameWithoutServer)
	result, err := tx.Exec(qStr)
	if err != nil {
		logger.Error("error while dumping to the table", tablename, "from csv", remoteFileNameWithoutServer)
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

	//removing the file from remote
	logger.Info("removing the data file from the remote server")
	rmCmd := exec.Command("ssh", remoteFileNameSplitted[0], "rm", remoteFileNameWithoutServer)
	err = rmCmd.Run()
	if err != nil {
		logger.Error("error removing the file from the server after dumping csv to the datastore", remoteFileName)
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
