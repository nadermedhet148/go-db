package storage_manager

import "fmt"

type WAL struct {
	FileManager *FileManager
}

// NewWAL creates a new Write-Ahead Log (WAL) file.
func NewWAL(fileName string) (*WAL, error) {

	// Create the file if it does not exist
	fileManager := NewFileManager("/home/nader/projects/go-db/test", fileName+".txt")
	err := fileManager.CreateIfNotExists()
	if err != nil {
		return nil, err
	}

	return &WAL{
		FileManager: fileManager,
	}, nil
}

// StartTransaction initializes a new transaction in the WAL with a transaction ID.
func (wal *WAL) StartTransaction(trxID string) error {
	// Write a transaction start marker with the transaction ID to the WAL
	startMarker := []byte(fmt.Sprintf("TRANSACTION_START:%s", trxID))
	return wal.WriteEntry(startMarker)
}

// EndTransaction finalizes a transaction in the WAL with a transaction ID.
func (wal *WAL) EndTransaction(trxID string) error {
	// Write a transaction end marker with the transaction ID to the WAL
	endMarker := []byte(fmt.Sprintf("TRANSACTION_END:%s", trxID))
	return wal.WriteEntry(endMarker)
}

// WriteEntry writes an entry to the WAL.
func (wal *WAL) WriteEntry(data []byte) error {
	return wal.FileManager.WriteToFile(data)
}

// WriteTransactionEntry writes a transaction-specific entry to the WAL.
func (wal *WAL) WriteTransactionEntry(trxID string, data []byte) error {
	entry := []byte(fmt.Sprintf("TRANSACTION:%s:%s", trxID, string(data)))
	return wal.FileManager.WriteToFile(entry)
}

// ReadEntry reads an entry from the WAL.
func (wal *WAL) ReadEntries() ([]byte, error) {
	return wal.FileManager.ReadFromFile()
}
