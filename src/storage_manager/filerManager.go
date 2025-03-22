package storage_manager

import (
	"os"
	"sync"
)

type FileManager struct {
	mu       sync.Mutex
	fileName string
	filePath string
	ext      string
}

func NewFileManager(filePath, fileName string, ext string) *FileManager {
	fileManager := FileManager{
		filePath: filePath,
		fileName: fileName,
		mu:       sync.Mutex{},
		ext:      ext,
	}
	fileManager.CreateIfNotExists()
	return &fileManager
}

// CreateIfNotExists creates the file if it does not exist.
func (fm *FileManager) CreateIfNotExists() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	_, err := os.Stat(fm.getFileName())
	if os.IsNotExist(err) {
		file, err := os.Create(fm.getFileName())
		if err != nil {
			return err
		}
		defer file.Close()
	}
	return nil
}

// WriteToFile writes data to a file in a thread-safe manner.
func (fm *FileManager) WriteToFile(data []byte) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := os.OpenFile(fm.getFileName(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(append(data, '\n'))
	return err
}

func (fm *FileManager) getFileName() string {
	return fm.filePath + "/" + fm.fileName + "." + fm.ext
}

// UpdateFileContent replaces the entire content of the file with the provided data.
func (fm *FileManager) UpdateFileContent(data []byte) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := os.OpenFile(fm.getFileName(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

// ReadFromFile reads data from a file in a thread-safe manner.
func (fm *FileManager) ReadFromFile() ([]byte, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := os.Open(fm.getFileName())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	data := make([]byte, stat.Size())
	_, err = file.Read(data)
	return data, err
}

// ClearFile clears the content of the file in a thread-safe manner.
func (fm *FileManager) ClearFile() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := os.OpenFile(fm.getFileName(), os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	return nil

}

// GetFileSize returns the size of the file in bytes.
func (fm *FileManager) GetFileSize() (int64, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := os.Open(fm.getFileName())
	if err != nil {
		return 0, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}
