package utils

import (
	"log"
)

// HandleError logs the error and panics
func HandleError(err error) {
	if err != nil {
		log.Printf("Error: %v", err)
		panic(err)
	}
}
