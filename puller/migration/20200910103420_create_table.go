package migration

import (
	"database/sql"
	"github.com/jinzhu/gorm"
	"github.com/pressly/goose"
	"tezos_index/puller/models"
)

func init() {
	goose.AddMigration(Up20200910103420, Down20200910103420)
}

func Up20200910103420(tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	db, err := gorm.Open("mysql", tx)
	if err != nil {
		return err
	}
	err = db.AutoMigrate(
		&models.Account{}, &models.Block{}, &models.Chain{}, &models.Flow{},
		&models.Contract{}, &models.Op{}, &models.Supply{}, &models.BigMapItem{},
		&models.Election{}, &models.Proposal{}, &models.Vote{}, &models.Ballot{},
		&models.Income{}, &models.Right{}, &models.Snapshot{}, &models.HarvesterStatus{}).Error
	return err
}

func Down20200910103420(tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	db, err := gorm.Open("mysql", tx)
	if err != nil {
		return err
	}
	err = db.DropTableIfExists(&models.Account{}, &models.Block{}, &models.Chain{}, &models.Flow{},
		&models.Contract{}, &models.Op{}, &models.Supply{}, &models.BigMapItem{},
		&models.Election{}, &models.Proposal{}, &models.Vote{}, &models.Ballot{},
		&models.Income{}, &models.Right{}, &models.Snapshot{}, &models.HarvesterStatus{}).Error
	return err
}
