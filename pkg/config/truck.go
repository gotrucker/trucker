package config

import (
	"log"
	"os"

	"path/filepath"
)

type Truck struct {
	Name string
	Input struct {
		Connection string `yaml:"connection,omitempty"`
		Table      string `yaml:"table,omitempty"`
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

	trucks := make([]Truck, 1, 1)
	for i, ymlPath := range ymlPaths {
		trucks[i] = loadTruck(ymlPath, cfg)
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

	inputSqlBuf, err := os.ReadFile(filepath.Join(dir, "input.sql"))
	if err == nil {
		truck.Input.Sql = string(inputSqlBuf)
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

	log.Printf(
		"Configured truck: %s:%s -> %s\nInput SQL:\n%s\nOutput SQL:\n%s\n\n",
		truck.Input.Connection,
		truck.Input.Table,
		truck.Output.Connection,
		truck.Input.Sql,
		truck.Output.Sql,
	)

	return truck
}
