package main

import (
	"path/filepath"
	"runtime"

	"github.com/tonyfg/trucker/pkg/mainroutines"
)

var (
	_, b, _, _ = runtime.Caller(0)
	Basepath   = filepath.Dir(b)
)

func startTrucker(project string) chan struct{} {
	_, _, trucksByInputConnection := mainroutines.Start(Basepath + "/../fixtures/projects/" + project)
	exitChan := make(chan struct{})

	go func() {
		<-exitChan

		for _, trucks := range trucksByInputConnection {
			for _, truck := range trucks {
				truck.Stop()
			}
		}
	}()

	return exitChan
}
