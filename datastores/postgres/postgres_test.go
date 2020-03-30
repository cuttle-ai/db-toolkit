// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package postgres has the datastore implementation for the postgres database to store data in cuttle platform
package postgres_test

import (
	"os"
	"testing"

	"github.com/cuttle-ai/brain/env"
	"github.com/cuttle-ai/brain/log"
	"github.com/cuttle-ai/db-toolkit/datastores/postgres"
	"github.com/cuttle-ai/octopus/interpreter"
)

func TestDumpCSV(t *testing.T) {
	l := log.NewLogger()
	env.LoadEnv(l)
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUsername := os.Getenv("DB_USERNAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	testdataFile := os.Getenv("TESTDATA_FILE_LOCATION")

	conn, err := postgres.NewPostgres(dbHost, dbPort, dbName, dbUsername, dbPassword)
	if err != nil {
		t.Error("error in connecting to the datastore", err)
		return
	}

	conn.DB.Exec("drop table if exists groceries")

	err = conn.DumpCSV(testdataFile, "groceries", []interpreter.ColumnNode{
		{Name: "item"},
		{Name: "brand"},
		{Name: "quantity", DataType: interpreter.DataTypeInt},
	}, false, true, l)

	if err != nil {
		//error while dumping csv
		t.Error("error while dumping the csv to datastore", err)
		return
	}
}
