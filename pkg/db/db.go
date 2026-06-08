package db

import (
	"fmt"
	"time"
)

const (
	Insert uint8 = iota
	Update
	Delete
)

type Column struct {
	Name string
	Type uint8
}

type Changes struct {
	Table     string
	Operation uint8 // Insert, Update, or Delete
	Columns   []Column
	Rows      chan [][]any
}

type Transaction struct {
	StreamPosition uint64
	CommitTime     time.Time // zero if the source didn't supply a commit timestamp
	Changes        chan *Changes
}

type Reader interface {
	Read(changes *Changes) *Changes
	Close()
}

type Writer interface {
	SetupPositionTracking()
	SetCurrentPosition(lsn uint64)
	GetCurrentPosition() uint64
	Write(changes *Changes) bool
	TruncateTable(table string)
	Close()
}

func OperationStr(operation uint8) string {
	switch operation {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	default:
		panic(fmt.Sprintf("Unknown operation %d\n", operation))
	}
}
