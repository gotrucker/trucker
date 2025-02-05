package config

import (
	"log"
	"os"

	"path/filepath"
)

type Truck struct {
	Name  string
	Input struct {
		Connection string   `yaml:"connection,omitempty"`
		Table      string   `yaml:"table,omitempty"`
		Tables     []string `yaml:"tables,omitempty"`
		Sql        string
	} `yaml:"input"`
	Output struct {
		Connection string `yaml:"connection,omitempty"`
		Sql        string
	} `yaml:"output"`
}

func LoadTrucks(projectPath string, cfg Config) []Truck {
	ymlPaths, err := filepath.Glob(filepath.Join(projectPath, "*", "truck.yml"))
	if err != nil {
		log.Fatal(err)
	}

	trucks := make([]Truck, 0, 1)
	for _, ymlPath := range ymlPaths {
		trucks = append(trucks, loadTruck(ymlPath, cfg))
	}

	return trucks
}

func loadTruck(path string, cfg Config) Truck {
	envMap, err := envToMap()
	if err != nil {
		log.Fatal(err)
	}

	truck := loadYml(path, Truck{}, envMap)
	dir := filepath.Dir(path)
	truck.Name = filepath.Base(dir)

	inputSqlPath := filepath.Join(dir, "input.sql")
	if _, err := os.Stat(inputSqlPath); err == nil {
		inputSqlBuf, err := os.ReadFile(inputSqlPath)
		if err == nil {
			truck.Input.Sql = string(inputSqlBuf)
		}
	}

	outputSqlBuf, err := os.ReadFile(filepath.Join(dir, "output.sql"))
	if err == nil {
		truck.Output.Sql = string(outputSqlBuf)
	}

	if connectionCfg, ok := cfg.Connections[truck.Input.Connection]; ok {
		cfg.Connections[truck.Input.Connection] = connectionCfg
	}

	if connectionCfg, ok := cfg.Connections[truck.Output.Connection]; ok {
		cfg.Connections[truck.Output.Connection] = connectionCfg
	}

	if truck.Input.Table != "" {
		truck.Input.Tables = append(truck.Input.Tables, truck.Input.Table)
	}
	log.Printf("[Truck %s] configured for:\n", truck.Name)

	for _, table := range truck.Input.Tables {
		log.Printf("- %s:%s -> %s\n", truck.Input.Connection, table, truck.Output.Connection)
	}

	return truck
}
