package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"personalMonorepo/distributedDataStore/contract"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

type Database struct {
	data        map[string][]byte
	writeAhead  []*contract.LogEntry
	logFile     string
	logFileLock sync.Mutex
	logFilePtr  *os.File
	logFileSize int64
	rotateSize  int64
}

const (
	INSERT = iota
	UPDATE
	DELETE
)

func NewDatabase(logFile string, rotateSize int64) *Database {
	return &Database{
		data:        make(map[string][]byte),
		writeAhead:  make([]*contract.LogEntry, 0),
		logFile:     logFile,
		logFileSize: 0,
		rotateSize:  rotateSize,
	}
}

func (db *Database) OpenLogFile() error {
	file, err := os.OpenFile(db.logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	db.logFilePtr = file
	return nil
}

func (db *Database) CloseLogFile() error {
	if db.logFilePtr == nil {
		return nil
	}

	err := db.logFilePtr.Close()
	if err != nil {
		return err
	}

	db.logFilePtr = nil
	return nil
}

func (db *Database) Set(key string, value []byte) error {
	db.logFileLock.Lock()
	defer db.logFileLock.Unlock()

	// Check if key exists
	val, ok := db.data[key]

	var logEntry *contract.LogEntry
	if ok && !bytes.Equal(val, value) {
		logEntry = &contract.LogEntry{
			Op:    UPDATE,
			Key:   key,
			Value: value,
		}
	} else if !ok {
		logEntry = &contract.LogEntry{
			Op:    INSERT,
			Key:   key,
			Value: value,
		}
	} else {
		// Value is the same, we don't want to append log or update in-memory database
		return nil
	}
	// Create log entry

	db.writeAhead = append(db.writeAhead, logEntry)

	// Update in-memory database
	db.data[key] = value

	// Write log entry to log file
	if db.logFilePtr != nil {
		logData, err := proto.Marshal(logEntry)
		if err != nil {
			return err
		}

		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(len(logData)))

		_, err = db.logFilePtr.Write(buf)
		if err != nil {
			return err
		}

		_, err = db.logFilePtr.Write(logData)
		if err != nil {
			return err
		}

		// Update log file size
		db.logFileSize += int64(len(logData) + 4)

		// Check if log file size exceeds the rotate threshold
		if db.logFileSize >= db.rotateSize {
			db.rotateLogFile()
		}
	}

	return nil
}

func (db *Database) rotateLogFile() {
	err := db.CloseLogFile()
	if err != nil {
		return
	}

	// Rename the current log file
	timestamp := time.Now().Format("20060102_150405")
	rotatedFile := fmt.Sprintf("%s_%s", db.logFile, timestamp)
	err = os.Rename(db.logFile, rotatedFile)
	if err != nil {
		log.Fatal(err)
	}

	// Open a new log file
	err = db.OpenLogFile()
	if err != nil {
		log.Fatal(err)
	}

	// Reset log file size
	db.logFileSize = 0
}

func (db *Database) Get(key string) ([]byte, error) {
	value, ok := db.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	return value, nil
}

func (db *Database) Delete(key string) {
	db.logFileLock.Lock()
	defer db.logFileLock.Unlock()

	delete(db.data, key)
}

func (db *Database) ReplayWriteAheadLog() error {
	sugar := zap.L().Sugar()
	sugar.Infof("Replaying write-ahead log")

	db.logFileLock.Lock()
	defer db.logFileLock.Unlock()

	file, err := os.Open(db.logFile)
	if err != nil {
		return err
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			sugar.Fatal(err)
		}
	}(file)

	var offset int64

	for {
		// reading the length of the encoded item before reading each item
		buf := make([]byte, 4)
		if _, err := file.ReadAt(buf, offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		itemSize := binary.LittleEndian.Uint32(buf)
		offset += 4

		// reading the actual encoded item
		item := make([]byte, itemSize)
		if _, err := file.ReadAt(item, offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		entry := &contract.LogEntry{}
		err := proto.Unmarshal(item, entry)
		if err != nil {
			return err
		}

		db.data[entry.Key] = entry.Value
		offset += int64(itemSize)
	}

	return nil
}

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logger)

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	sugar := logger.Sugar()

	logFile := "database.bin"
	db := NewDatabase(logFile, 32)

	// Open the write-ahead log file
	err = db.OpenLogFile()
	if err != nil {
		sugar.Fatal(err)
	}
	defer func(db *Database) {
		err := db.CloseLogFile()
		if err != nil {
			sugar.Fatal(err)
		}
	}(db)

	// Replay the write-ahead log
	err = db.ReplayWriteAheadLog()
	if err != nil {
		sugar.Fatal(err)
	}

	// Set values in the database
	err = db.Set("name", []byte("John"))
	if err != nil {
		sugar.Fatal(err)
	}

	err = db.Set("age", []byte("30"))
	if err != nil {
		sugar.Fatal(err)
	}

	err = db.Set("city", []byte("New York"))
	if err != nil {
		sugar.Fatal(err)
	}

	// Get values from the database
	name, _ := db.Get("name")
	age, _ := db.Get("age")
	city, _ := db.Get("city")

	fmt.Println("Name:", string(name))
	fmt.Println("Age:", string(age))
	fmt.Println("City:", string(city))

	// Delete a key from the database
	db.Delete("age")

	// Periodically flush the write-ahead log to disk
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			db.logFileLock.Lock()

			logData := make([]byte, 0)
			for _, entry := range db.writeAhead {
				entryData, err := proto.Marshal(entry)
				if err != nil {
					sugar.Fatal(err)
				}
				logData = append(logData, entryData...)
			}

			// Write log data to the log file
			_, err := db.logFilePtr.Write(logData)
			if err != nil {
				sugar.Fatal(err)
			}

			err = db.logFilePtr.Sync()
			if err != nil {
				sugar.Fatal(err)
				return
			}

			db.logFileLock.Unlock()
		}
	}()

	// Wait for a key press to exit the program
	fmt.Println("\nPress Enter to exit...")
	_, err = fmt.Scanln()
	if err != nil {
		sugar.Fatal(err)
	}
	ticker.Stop()
}
