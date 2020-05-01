// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package dataset has the utilities to make the datasets efficient.
//It has utilities like post processing to identify the dimensions in the dataset
//To identify the date columns
package dataset

import (
	"strconv"

	"github.com/cuttle-ai/brain/log"
	"github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/db-toolkit/datastores/services"
	"github.com/cuttle-ai/octopus/interpreter"
	"github.com/jinzhu/gorm"
)

//IdentifyDimensions will identify the dimensions in a given dataset and update the same in the db
func IdentifyDimensions(l log.Log, conn *gorm.DB, cols []models.Node, table models.Node, dSer services.Service, dt *models.Dataset) error {
	/*
	 * we will first get the columns that are not of the type dimension
	 * We will get the datastore in which the table is store in
	 * Then we will iterate through the columns
	 *		and check the number of unique values the column is holding.
	 * 		based that we will update the dimension type
	 * For the columns with udpated dimension type, we will update them in the db
	 */
	//filter columns of not type dimension
	columns := []interpreter.ColumnNode{}
	colMap := map[string]models.Node{}
	for _, v := range cols {
		colMap[v.UID.String()] = v
		iN := v.ColumnNode()
		if !iN.Dimension {
			columns = append(columns, iN)
		}
	}
	if len(columns) == 0 {
		return nil
	}

	//getting the datastore in which the table is stored in
	dStore, err := dSer.Datastore()
	if err != nil {
		//error while getting the datastore
		l.Error("error while getting the datastore", dSer.ID)
		return err
	}

	//iterating through the columns to identify whether they are of type dimension
	dimCols := []models.Node{}
	tb := table.TableNode()
	for i := 0; i < len(columns); i++ {
		//get the unique values the columns are holding
		result, err := dStore.Exec("SELECT COUNT(DISTINCT(\"" + columns[i].Name + "\")) FROM \"" + tb.Name + "\"")
		if err != nil {
			//error while querying the datastore to find the count of the unique values in the column
			l.Error("error while querying the datastore to find the count of the unique values in the column", columns[i].Name, "from the table", tb.Name)
			l.Error(err)
			continue
		}
		count := 0
		for _, row := range result {
			for _, val := range row {
				str, _ := val.(*string)
				count, _ = strconv.Atoi(*str)
			}
		}

		//update the dimension type if required
		if count > 0 && count < 50 {
			columns[i].Dimension = true
			c, _ := colMap[columns[i].UID]
			dimCols = append(dimCols, c.FromColumn(columns[i]))
		}
	}
	l.Info("have got", len(dimCols), "columns that are of type dimension")

	//for the updated columns, update the info in the db
	_, err = dt.UpdateColumns(l, conn, dimCols)
	if err != nil {
		//error while updating the dimension status of the columns
		l.Error("error while updating the dimension status of the columns of the table", tb.Name)
		return err
	}

	return nil
}

//IdentifyDates will identify the dates in the datsets and update the same in the db
func IdentifyDates(l log.Log, conn *gorm.DB, cols []models.Node, table models.Node) error {
	return nil
}

//OptimizeDatasetMetadata will optimize metadata associated with a dataset
//It will find the dimension columns in the dataset
//It will also try to identify the date columns in the datasets
func OptimizeDatasetMetadata(l log.Log, conn *gorm.DB, id uint, dSer services.Service, userID uint) error {
	/*
	 * We will get the dataset info from the db
	 * Then we will get the columns in the dataset
	 * Then we will get the table in the dataset
	 * Then we will identify the dimensions in the dataset
	 * Then we will identify the dates in the dataset
	 */
	dt := &models.Dataset{Model: gorm.Model{ID: id}, UserID: userID}
	l.Info("going to optimize the dataset metadata for", dt.ID)

	//getting the dataset info from the database
	err := dt.Get(conn)
	if err != nil {
		//error while finding the dataset info from the db
		l.Error("error while finding the dataset info from the db")
		return err
	}

	//get the columns in the dataset
	cols, err := dt.GetColumns(conn)
	if err != nil {
		//error while finding the columns in the dataset
		l.Error("error while finding the columns in the dataset from the db")
		return err
	}

	//get the table in the dataset
	table, err := dt.GetTable(conn)
	if err != nil {
		//error while finding the table in the dataset
		l.Error("error while finding the table in the dataset from the db")
		return err
	}

	//identify the dimensions in the dataset
	err = IdentifyDimensions(l, conn, cols, table, dSer, dt)
	if err != nil {
		//error while identifying the dimension columns in the dataset
		l.Error("error while identifying the dimension columns in the dataset")
		return err
	}

	//identify the dates in the dataset
	err = IdentifyDates(l, conn, cols, table)
	if err != nil {
		//error while identifying the date columns in the dataset
		l.Error("error while identifying the date columns in the dataset")
		return err
	}

	l.Info("successfully optimized the dataset metadata for", dt.ID)
	return nil
}
