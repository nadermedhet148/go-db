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

func TestConcurrentWriteAndRead(t *testing.T) {
	tableManager := data_manager.NewTableManager("concurrent_test")

	data := map[string]interface{}{
		"key1":  "value1",
		"key2":  "value2",
		"key3":  "value3",
		"key4":  "value4",
		"key5":  "value5",
		"key6":  "value6",
		"key7":  "value7",
		"key8":  "value8",
		"key9":  "value9",
		"key10": "value10",
	}

	// Create a worker pool with 10 workers for concurrent writes
	writeCh := make(chan struct{}, 10)
	for key, value := range data {
		writeCh <- struct{}{}
		go func(k string, v interface{}) {
			defer func() { <-writeCh }()
			jsonData, err := json.Marshal(map[string]interface{}{k: v})
			utils.HandleError(err)
			tableManager.WriteToTable(jsonData)
		}(key, value)
	}

	// Wait for all writes to complete
	for i := 0; i < cap(writeCh); i++ {
		writeCh <- struct{}{}
	}

	readData := tableManager.ReadAllFromTable()

	for key := range readData {
		val := tableManager.ReadFromTable(key)
		assert.Equal(t, readData[key], val)
		break
	}

}
