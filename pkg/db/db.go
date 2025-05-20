package db

import (
	"fmt"
	"iter"
	"log/slog"
	"os"
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

type Changeset struct {
	Table           string
	Operation       uint8 // Insert, Update, or Delete
	Columns         []Column
	Rows            [][]any
	StreamPosition  uint64
}

type ChanChangeset struct {
	Table     string
	Operation uint8 // Insert, Update, or Delete
	Columns   []Column
	Rows      chan [][]any
}

type Transaction struct {
	StreamPosition   uint64
	Changesets iter.Seq[*Changeset]
}

type Reader interface {
	Read(changeset *Changeset) *ChanChangeset
	Close()
}

type Writer interface {
	SetupPositionTracking()
	SetCurrentPosition(lsn uint64)
	GetCurrentPosition() uint64
	Write(changeset *ChanChangeset)
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
		slog.Error("db", "msg", fmt.Sprintf("Unknown operation %d", operation))
		os.Exit(1)
		return "" // Go compiler goes nuts if we don't return something
	}
}
