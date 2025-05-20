package config

import (
	"fmt"
	"log/slog"
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
		slog.Error("config", "msg", err)
		os.Exit(1)
	}

	trucks := make([]Truck, 0, 1)
	for _, ymlPath := range ymlPaths {
		trucks = append(trucks, loadTruck(ymlPath, cfg))
	}

	slog.Info("config", "msg", fmt.Sprintf("Loaded %d trucks", len(trucks)))
	return trucks
}

func loadTruck(path string, cfg Config) Truck {
	envMap, err := envToMap()
	if err != nil {
		slog.Error("config", "msg", err)
		os.Exit(1)
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

	return truck
}
