package db

import "fmt"

const (
	Insert uint8 = iota
	Update
	Delete
)

type Column struct {
	Name string
	Type uint8
}

type Reader interface {
	Read(operation uint8, columns []Column, rowValues [][]any) ([]Column, [][]any)
	Close()
}

type Writer interface {
	SetupPositionTracking()
	SetCurrentPosition(lsn uint64)
	GetCurrentPosition() uint64
	Write(operation uint8, columns []Column, values [][]any)
	TruncateTable(table string)
	WithTransaction(f func())
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
