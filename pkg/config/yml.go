package config

import (
	"bytes"
	"log"
	"os"
	"strings"

	"text/template"

	"gopkg.in/yaml.v2"
)

func loadYml[Config any](path string, config Config, variables map[string]string) Config {
	dat, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	tmpl, err := template.New("connectionsConfig").Parse(string(dat))
	if err != nil {
		log.Fatal(err)
	}

	buf := new(bytes.Buffer)
	err = tmpl.Execute(buf, variables)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(buf.Bytes(), &config)
	if err != nil {
		log.Fatal(err)
	}

	return config
}

func envToMap() (map[string]string, error) {
	envMap := make(map[string]string)
	var err error

	for _, v := range os.Environ() {
		split_v := strings.Split(v, "=")
		envMap[split_v[0]] = strings.Join(split_v[1:], "=")
	}
	return envMap, err
}
