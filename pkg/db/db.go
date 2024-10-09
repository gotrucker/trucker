package db

type ConnectionPool interface {
	Query(sql string, args ...any) ([]map[string]any, error)
	Disconnect()
	ConcretePool() any
}

type ReplicationConn interface {
}

type Db struct {
	Read ConnectionPool
	Write ConnectionPool
}
