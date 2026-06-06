//go:build gnarly

package main

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"

	"github.com/tonyfg/trucker/pkg/mainroutines"
)

var (
	_, b, _, _ = runtime.Caller(0)
	Basepath   = filepath.Dir(b)
)

func startTrucker(project string) func() {
	_, _, trucksByInputConnection, rcClients := mainroutines.Start(Basepath + "/../fixtures/projects/" + project)

	return func() {
		for _, trucks := range trucksByInputConnection {
			for _, truck := range trucks {
				truck.Stop()
			}
		}
		for _, rc := range rcClients {
			rc.Close()
			<-rc.WaitDone()
		}
	}
}

func copyFile(src, dst string) error {
	srcfd, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcfd.Close()

	dstfd, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstfd.Close()

	if _, err = io.Copy(dstfd, srcfd); err != nil {
		return err
	}
	srcinfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	return os.Chmod(dst, srcinfo.Mode())
}

func copyDir(src string, dst string) error {
	srcinfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(dst, srcinfo.Mode()); err != nil {
		return err
	}
	fds, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, fd := range fds {
		srcfp := path.Join(src, fd.Name())
		dstfp := path.Join(dst, fd.Name())
		if fd.IsDir() {
			if err = copyDir(srcfp, dstfp); err != nil {
				panic(err)
			}
		} else {
			if err = copyFile(srcfp, dstfp); err != nil {
				panic(err)
			}
		}
	}
	return nil
}
