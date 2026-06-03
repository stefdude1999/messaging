package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type WAL struct {
	directory           string
	file                *os.File
	mu                  sync.Mutex
	lastSequenceNo      uint64
	bufWriter           *bufio.Writer
	syncTimer           *time.Timer
	shouldFsync         bool
	maxFileSize         int64
	maxSegments         int
	currentSegmentIndex int
	pending             map[string]Message
	ctx                 context.Context
	cancel              context.CancelFunc
}

// write to log
// write to WAL before sending
// remove from WAL after receiving
// on creating broker: Read WAL if exists
// create subscriber: check if any messages left to fire off

func CreateWAL(directory string, enableFsync bool) (*WAL, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	pending := make(map[string]Message)

	segmentPath := filepath.Join(directory, "segment-0.log")

	// Read existing messages into pending if file already exists
	if existingData, err := os.ReadFile(segmentPath); err == nil {
		scanner := bufio.NewScanner(strings.NewReader(string(existingData)))
		for scanner.Scan() {
			var msg Message
			if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil && msg.GUID != "" {
				pending[msg.GUID] = msg
			}
		}
	}

	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &WAL{
		directory:   directory,
		shouldFsync: enableFsync,
		file:        file,
		bufWriter:   bufio.NewWriter(file),
		ctx:         ctx,
		cancel:      cancel,
		pending:     pending,
	}, nil

}

func (w *WAL) Write(msg Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		file, err := os.OpenFile(filepath.Join(w.directory, "segment-0.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		w.file = file
		w.bufWriter = bufio.NewWriter(file)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if _, err := w.bufWriter.Write(append(data, '\n')); err != nil {
		return err
	}

	if err := w.Sync(); err != nil {
		return err
	}

	w.pending[msg.GUID] = msg
	return nil
}

func (wal *WAL) Sync() error {
	if err := wal.bufWriter.Flush(); err != nil {
		return err
	}
	if wal.shouldFsync {
		if err := wal.file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) Remove(guid string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.pending[guid]; !ok {
		return nil
	}

	delete(w.pending, guid)

	if len(w.pending) == 0 {
		w.file.Close()
		w.file = nil
		return os.Remove(filepath.Join(w.directory, "segment-0.log"))
	}

	// Rewrite file without the removed entry
	w.file.Close()
	segmentPath := filepath.Join(w.directory, "segment-0.log")
	file, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	w.file = file
	w.bufWriter = bufio.NewWriter(file)

	for _, msg := range w.pending {
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		if _, err := w.bufWriter.Write(append(data, '\n')); err != nil {
			return err
		}
	}

	return w.Sync()
}

func (w *WAL) PendingForTopic(topic string) []Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	var result []Message
	for _, msg := range w.pending {
		if msg.Topic == topic {
			result = append(result, msg)
		}
	}
	return result
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		if err := w.Sync(); err != nil {
			return err
		}
		return w.file.Close()
	}
	return nil
}
