package fileManager_test

import (
	"db/manager/v2/src/data_manager"
	"db/manager/v2/src/utils"
	"encoding/json"
	"testing"
)

func TestCreateFile(t *testing.T) {

	tableManager := data_manager.NewTableManager("test")
	data := map[string]interface{}{
		"key": "value",
		"num": 42,
	}
	jsonData, err := json.Marshal(data)
	utils.HandleError(err)

	for i := 0; i < 100000; i++ {
		tableManager.WriteToTable(jsonData)
	}

}
