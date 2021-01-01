package index

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"tezos_index/puller/models"
	"time"
)
import _ "github.com/jinzhu/gorm/dialects/mysql"

func TestAccountIndex_DB(t *testing.T) {
	dsn := "root:WcGsHDMBmcv7mc#QWkuR@tcp(127.0.0.1:3306)/sandy_test?charset=utf8mb4&parseTime=True&loc=Local"
	db := InitDB(dsn)
	db.AutoMigrate(&models.Right{})
	startTime := time.Now()
	rights := make([]*models.Right, 0, 10000)
	for i := 0; i < 100; i++ {
		num := rand.Intn(99999)
		right := &models.Right{
			Type:           1,
			Height:         int64(2 * num),
			Cycle:          int64(num),
			Priority:       num,
			AccountId:      models.AccountID(num / 2),
			IsLost:         true,
			IsStolen:       false,
			IsSeedRevealed: true,
		}
		rights = append(rights, right)
	}
	batch := 5
	err := BatchInsertRights(rights, batch, db)
	assert.NoError(t, err)

	spendTime := time.Since(startTime).Seconds()
	t.Log(spendTime)
}

func TestAccountIndex_ConnectBlock(t *testing.T) {
	dsn := "root:WcGsHDMBmcv7mc#QWkuR@tcp(127.0.0.1:3306)/sandy_test?charset=utf8mb4&parseTime=True&loc=Local"
	db := InitDB(dsn)
	db.AutoMigrate(&models.Right{})
	tx01 := db.Begin()

	// err := tx01.Update(models.Right{RowId: 1, Height: 11}).Error
	newRight := models.Right{Height: 222}
	err := tx01.Create(&newRight).Error
	assert.NoError(t, err)

	res := &models.Right{}
	err = tx01.First(res).Error
	assert.NoError(t, err)
	t.Log(res.Height)
	// tx01.Commit()

	err = tx01.Model(&models.Right{}).Where("height = 9").First(res).Error
	assert.NoError(t, err)
	// tx01.Commit()
}
