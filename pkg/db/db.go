package db

import (
	"fmt"
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

type Change struct {
	Table     string
	Operation uint8 // Insert, Update, or Delete
	Columns   []Column
	Rows      chan [][]any
}

type Transaction struct {
	StreamPosition uint64
	Changes        chan *Change
}

type Reader interface {
	Read(changeset *Change) *Change
	Close()
}

type Writer interface {
	SetupPositionTracking()
	SetCurrentPosition(lsn uint64)
	GetCurrentPosition() uint64
	Write(changeset *Change)
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
