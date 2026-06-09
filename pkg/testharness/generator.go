package testharness

import (
	"bytes"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/tonyfg/trucker/pkg/config"
)

//go:embed templates/*.sql
var templateFS embed.FS

func Generate(projectPath, truckName, testName string) error {
	if err := validateName("truck", truckName); err != nil {
		return err
	}
	if err := validateName("test", testName); err != nil {
		return err
	}

	cfg := config.Load(filepath.Join(projectPath, "trucker.yml"))
	truckCfg, err := loadTruckByName(projectPath, cfg, truckName)
	if err != nil {
		return err
	}
	if len(truckCfg.Input.Tables) == 0 {
		return fmt.Errorf("truck %q has no input tables", truckName)
	}

	testDir := filepath.Join(projectPath, truckName, "tests", testName)
	if _, err := os.Stat(testDir); err == nil {
		return fmt.Errorf("test directory already exists: %s", testDir)
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.MkdirAll(testDir, 0755); err != nil {
		return err
	}

	vars := map[string]string{"InputTable": truckCfg.Input.Tables[0]}
	for _, name := range []string{"input_db_seed.sql", "output_db_seed.sql", "stream_statements.sql", "expectations.sql"} {
		contents, err := renderTemplate(name, vars)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(testDir, name), []byte(contents), 0644); err != nil {
			return err
		}
	}

	return nil
}

func renderTemplate(name string, vars any) (string, error) {
	raw, err := templateFS.ReadFile(filepath.Join("templates", name))
	if err != nil {
		return "", err
	}
	tmpl, err := template.New(name).Parse(string(raw))
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, vars); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func validateName(label, name string) error {
	if name == "" {
		return fmt.Errorf("%s name cannot be empty", label)
	}
	if strings.Contains(name, "/") || strings.Contains(name, `\\`) || name == "." || name == ".." || strings.Contains(name, "..") {
		return fmt.Errorf("invalid %s name %q", label, name)
	}
	return nil
}

func loadTruckByName(projectPath string, cfg config.Config, truckName string) (config.Truck, error) {
	for _, truckCfg := range config.LoadTrucks(projectPath, cfg) {
		if truckCfg.Name == truckName {
			return truckCfg, nil
		}
	}
	return config.Truck{}, fmt.Errorf("truck %q not found", truckName)
}
