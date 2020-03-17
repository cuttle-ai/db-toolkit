// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package services has the service models for storing the datastore service info
package services

import (
	"github.com/jinzhu/gorm"
)

//Service is defnition of the datastore service
type Service struct {
	gorm.Model
	//URL at which the service is available
	URL string
	//Port is the port at which the datastore service is available
	Port string
	//Username for authentication with the datastore service
	Username string
	//Password for authentication with the datastore service
	Password string
	//Name is the name of the datastore db
	Name string
	//Group to which the datastore belongs to classify the service
	Group string
}

//GetAll returns the list of datastore available
func GetAll(conn *gorm.DB) ([]Service, error) {
	result := []Service{}
	err := conn.Find(&result).Error
	return result, err
}

//Get will set the info of the service for the give id from database
func (s *Service) Get(conn *gorm.DB) error {
	return conn.Find(s).Error
}

//Create will create a given service
func (s *Service) Create(conn *gorm.DB) error {
	return conn.Create(s).Error
}

//Update will update a given service
func (s *Service) Update(conn *gorm.DB) error {
	return conn.Save(s).Error
}

//Delete will delete a given service
func (s *Service) Delete(conn *gorm.DB) error {
	return conn.Delete(s).Error
}
