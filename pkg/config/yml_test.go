package config

import (
	"testing"
)

type testStruct struct {
	A string `yaml:"a,omitempty"`
	B int    `yaml:"b,omitempty"`
	C bool   `yaml:"c,omitempty"`
	D string `yaml:"d,omitempty"`
}

const ymlPath = "../../test/fixtures/test.yml"

func TestLoadYmlWithEnv(t *testing.T) {
	config := loadYml(
		ymlPath,
		testStruct{},
		map[string]string{"TEST_ENV_VAR": "some_value"},
	)

	if config.A != "b" {
		t.Error("Expected a = b, got", config.A)
	}

	if config.B != 33 {
		t.Error("Expected b = 33, got", config.B)
	}

	if config.C != true {
		t.Error("Expected c = true, got", config.C)
	}

	if config.D != "some_value" {
		t.Error("Expected d = some_value, got", config.D)
	}
}

func TestLoadYmlEnvDefaults(t *testing.T) {
	config := loadYml(
		ymlPath,
		testStruct{},
		map[string]string{},
	)

	if config.D != "default_value" {
		t.Error("Expected d = default_value, got", config.D)
	}
}
