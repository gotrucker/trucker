package db

type ConnectionPool interface {
	Query(query string) ([]map[string]any, error)
	Disconnect()
}

type Db struct {
	Read ConnectionPool
	Write ConnectionPool
}
