package data_manager

import (
	"db/manager/v2/src/storage_manager"
	"db/manager/v2/src/utils"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

type TableManager struct {
	tableName   string
	FileManager *storage_manager.FileManager
	WAL         *storage_manager.WAL
}

// NewTableManager creates a new TableManager with the specified table name.
func NewTableManager(tableName string) *TableManager {

	fileManager := storage_manager.NewFileManager("/home/nader/projects/go-db/test", tableName)
	wal, err := storage_manager.NewWAL(tableName)
	if err != nil {
		panic(err)
	}

	return &TableManager{tableName: tableName,
		FileManager: fileManager,
		WAL:         wal,
	}
}

func (tm *TableManager) WriteToTable(data []byte) {

	id := uuid.New().String()
	utils.HandleError(tm.WAL.StartTransaction(id))
	utils.HandleError(tm.WAL.WriteTransactionEntry(id, data))
	utils.HandleError(tm.WAL.EndTransaction(id))
}
func (tm *TableManager) FlushWalToTable() map[string]string {
	dataBytes, err := tm.WAL.ReadEntries()
	utils.HandleError(err)
	data := strings.Split(string(dataBytes), "\n")
	utils.HandleError(err)
	endedTransactions := make([]string, 0)

	// Group WAL entries by transaction ID
	groupedData := make(map[string]string)
	for _, entry := range data {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			continue
		}

		prefix := parts[0]
		content := parts[1]

		switch {
		case strings.HasPrefix(prefix, "TRANSACTION_START"):
			transactionID := content
			groupedData[transactionID] = ""
		case strings.HasPrefix(prefix, "TRANSACTION_END"):
			transactionID := content
			// Add the transaction ID to an array or list of ended transactions
			endedTransactions = append(endedTransactions, transactionID)
		case strings.HasPrefix(prefix, "TRANSACTION"):
			idParts := strings.SplitN(content, ":", 2)
			if len(idParts) != 2 {
				continue
			}
			transactionID := idParts[0]
			content := idParts[1]
			groupedData[transactionID] += content
		}
	}

	// Remove transactions that are not ended
	for transactionID := range groupedData {
		found := false
		for _, endedID := range endedTransactions {
			if transactionID == endedID {
				found = true
				break
			}
		}
		if !found {
			delete(groupedData, transactionID)
		}
	}

	tableData, err := tm.FileManager.ReadFromFile()

	utils.HandleError(err)

	var unmarshaledData map[string]interface{}
	if len(tableData) == 0 {
		unmarshaledData = make(map[string]interface{})
	} else {
		err = json.Unmarshal(tableData, &unmarshaledData)
		utils.HandleError(err)
	}

	for transactionID, content := range groupedData {
		unmarshaledData[transactionID] = content
	}

	updatedData, err := json.Marshal(unmarshaledData)
	utils.HandleError(err)

	utils.HandleError(tm.FileManager.UpdateFileContent(updatedData))

	return groupedData
}
