package engine

import (
	"encoding/json"
	"os"
	"sync"
)

type WAL struct {
	file    *os.File
	mu      sync.Mutex
	enabled bool
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file, enabled: true}, nil
}

func (w *WAL) Write(entry interface{}) error {
	if !w.enabled {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err = w.file.Write(append(data, '\n')); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	return w.file.Close()
}