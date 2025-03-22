package fileManager_test

import (
	"db/manager/v2/src/data_manager"
	"db/manager/v2/src/utils"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateFile(t *testing.T) {

	tableManager := data_manager.NewTableManager("test")
	data := map[string]interface{}{
		"key": "value",
		"num": 42,
	}
	jsonData, err := json.Marshal(data)
	utils.HandleError(err)

	tableManager.WriteToTable(jsonData)

	readData := tableManager.ReadAllFromTable()

	for key := range readData {
		val := tableManager.ReadFromTable(key)
		assert.Equal(t, readData[key], val)

		break
	}

}
