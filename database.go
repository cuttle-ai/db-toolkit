// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package toolkit contains structures and functions required for interacting with different databases
//sub packages has the drivers for interacting with different databases
package toolkit

import (
	"github.com/cuttle-ai/brain/log"
	"github.com/cuttle-ai/octopus/interpreter"
)

//Datastore can store data uploaded/imported by the user to cuttle platform
type Datastore interface {
	//DumpCSV will dump the given csv file to the datastore.
	//Default behaviour of the method will be to replace the existing data in the datastore.
	//But if appendData flag is set, then existing data won't be removed instead new data will be appended to it.
	DumpCSV(filename string, tablename string, columns []interpreter.ColumnNode, appendData bool, createTable bool, logger log.Log) error
	//DeleteTable will delete the given table in the datastore
	DeleteTable(tablename string) error
	//Exec can execute a query and return the response as the array of interfaces
	Exec(query string, args ...interface{}) ([]interface{}, error)
}
