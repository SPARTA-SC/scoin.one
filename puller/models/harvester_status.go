package models

import "github.com/jinzhu/gorm"

type HarvesterStatus struct {
	Key   string `gorm:"column:key;primary_key" json:"key"`
	Value string `gorm:"column:value" json:"value"`
	Notes string `gorm:"column:notes" json:"notes"`
}

func (HarvesterStatus) TableName() string {
	return "harvester_status"
}

func UpdateHarvesterStatus(db *gorm.DB, key, value string) error {
	sql := "INSERT INTO harvester_status (`key`, `value`) VALUES(?, ?) ON DUPLICATE KEY UPDATE `value` = ?"
	return db.Exec(sql, key, value, value).Error
}
