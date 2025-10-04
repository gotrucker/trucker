package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"path/filepath"
)

const DefaultSlowQueryThresholdMs = 1000 // Default slow query threshold in milliseconds

type connectionYml struct {
	Name         string `yaml:"name"`
	Adapter      string `yaml:"adapter"`
	Host         string `yaml:"host"`
	Port         uint16 `yaml:"port"`
	Ssl          string `yaml:"ssl"`
	Database     string `yaml:"database"`
	User         string `yaml:"user"`
	Pass         string `yaml:"pass"`
	HostPath     string `yaml:"host_path"`
	PortPath     string `yaml:"port_path"`
	SslPath      string `yaml:"ssl_path"`
	DatabasePath string `yaml:"database_path"`
	UserPath     string `yaml:"user_path"`
	PassPath     string `yaml:"pass_path"`
}

type configYml struct {
	UniqueId             string          `yaml:"unique_id"`
	SlowQueryThresholdMs int64           `yaml:"slow_query_threshold_ms"`
	Connections          []connectionYml `yaml:"connections"`
}

type Connection struct {
	Name     string
	Adapter  string
	Host     string
	Port     uint16
	Database string
	Ssl      string
	User     string
	Pass     string
}

type Config struct {
	UniqueId             string
	SlowQueryThresholdMs int64
	Connections          map[string]Connection
}

func Load(path string) Config {
	envMap, err := envToMap()
	if err != nil {
		log.Fatal(err)
	}

	configYml := loadYml(path, configYml{}, envMap)

	config := Config{
		UniqueId:             configYml.UniqueId,
		SlowQueryThresholdMs: configYml.SlowQueryThresholdMs,
		Connections:          make(map[string]Connection),
	}

	if config.SlowQueryThresholdMs == 0 {
		config.SlowQueryThresholdMs = DefaultSlowQueryThresholdMs
		log.Printf("Using default value of %dms as a threshold to log slow queries...", config.SlowQueryThresholdMs)
	}

	// TODO: validations

	basePath := filepath.Dir(filepath.Clean(path))

	for _, connYml := range configYml.Connections {
		connection := connectionYmlToConnection(connYml, basePath)
		config.Connections[connection.Name] = connection
	}

	log.Printf("%d DB connections configured.\n", len(config.Connections))
	return config
}

func connectionYmlToConnection(connYml connectionYml, basePath string) Connection {
	connection := Connection{
		Name:     connYml.Name,
		Adapter:  connYml.Adapter,
		Host:     connYml.Host,
		Port:     connYml.Port,
		Ssl:      connYml.Ssl,
		Database: connYml.Database,
		User:     connYml.User,
		Pass:     connYml.Pass,
	}

	if connYml.HostPath != "" {
		connection.Host = readFile(basePath, connYml.HostPath)
	}

	if connYml.PortPath != "" {
		portStr := readFile(basePath, connYml.PortPath)

		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			log.Fatal(err)
		}

		connection.Port = uint16(port)
	}

	if connYml.SslPath != "" {
		connection.Ssl = readFile(basePath, connYml.SslPath)
	}

	if connYml.DatabasePath != "" {
		connection.Database = readFile(basePath, connYml.DatabasePath)
	}

	if connYml.UserPath != "" {
		connection.User = readFile(basePath, connYml.UserPath)
	}

	if connYml.PassPath != "" {
		connection.Pass = readFile(basePath, connYml.PassPath)
	}

	return connection
}

func readFile(basePath string, path string) string {
	if strings.HasPrefix(path, "/") {
		basePath = ""
	}

	data, err := os.ReadFile(filepath.Join(basePath, path))
	if err != nil {
		log.Fatal(err)
	}

	return strings.TrimSpace(string(data))
}
