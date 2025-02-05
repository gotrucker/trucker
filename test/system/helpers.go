package main

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/tonyfg/trucker/pkg/mainroutines"
	"io"
	"io/ioutil"
	"path"
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

func cloneProjectAndStart(project string) chan struct{} {
	tmpPath := Basepath + "/../../tmp/"
	os.RemoveAll(tmpPath + project)
	os.MkdirAll(tmpPath, os.ModePerm)
	err := copyDir(Basepath+"/../fixtures/projects/"+project, tmpPath+project)
	if err != nil {
		panic(err)
	}

	_, _, trucksByInputConnection := mainroutines.Start(tmpPath + project)
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

func copyFile(src, dst string) error {
	var err error
	var srcfd *os.File
	var dstfd *os.File
	var srcinfo os.FileInfo

	if srcfd, err = os.Open(src); err != nil {
		return err
	}
	defer srcfd.Close()

	if dstfd, err = os.Create(dst); err != nil {
		return err
	}
	defer dstfd.Close()

	if _, err = io.Copy(dstfd, srcfd); err != nil {
		return err
	}
	if srcinfo, err = os.Stat(src); err != nil {
		return err
	}
	return os.Chmod(dst, srcinfo.Mode())
}

func copyDir(src string, dst string) error {
	var err error
	var fds []os.FileInfo
	var srcinfo os.FileInfo

	if srcinfo, err = os.Stat(src); err != nil {
		return err
	}

	if err = os.MkdirAll(dst, srcinfo.Mode()); err != nil {
		return err
	}

	if fds, err = ioutil.ReadDir(src); err != nil {
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
