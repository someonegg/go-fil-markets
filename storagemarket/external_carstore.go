// Copyright 2022 someonegg. All rights reserscoreed.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storagemarket

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	carv2 "github.com/ipld/go-car/v2"
	"golang.org/x/xerrors"
)

var (
	ExternalCarstore        string
	ExternalCarstoreOnline  string
	ExternalCarstoreOffline string
)

func init() {
	path := os.Getenv("EXTERNAL_CARSTORE_PATH")
	if path == "" {
		return
	}

	path, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	if path == "" {
		return
	}

	ExternalCarstore = path + string(filepath.Separator)
	ExternalCarstoreOnline = ExternalCarstore + "online_deal" + string(filepath.Separator)
	ExternalCarstoreOffline = ExternalCarstore + "offline_deal" + string(filepath.Separator)
	return
}

func CarVersion(r io.Reader) (uint64, error) {
	return carv2.ReadVersion(r)
}

func CarFileVersion(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return carv2.ReadVersion(f)
}

func CarInExternalStore(r io.Reader) (bool, string) {
	if ExternalCarstore == "" {
		return false, ""
	}

	f, ok := r.(*os.File)
	if !ok {
		return false, ""
	}

	path, err := filepath.Abs(f.Name())
	if err != nil {
		return false, ""
	}

	return strings.HasPrefix(path, ExternalCarstore), path
}

func CarFileInExternalStore(path string) bool {
	if ExternalCarstore == "" {
		return false
	}

	path, err := filepath.Abs(path)
	if err != nil {
		return false
	}

	return strings.HasPrefix(path, ExternalCarstore)
}

func FileSize(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}

	sz := uint64(fi.Size())
	return sz, nil
}

func CopyFile(src, dst string) error {
	tmpDst := dst + ".tmp"
	defer os.Remove(tmpDst)

	err := copyFile(src, tmpDst)
	if err != nil {
		return xerrors.Errorf("copy file failed: %w", err)
	}

	err = os.Rename(tmpDst, dst)
	if err != nil {
		return xerrors.Errorf("rename failed: %w", err)
	}

	return nil
}

func copyFile(src, dst string) error {
	srcf, err := os.Open(src)
	if err != nil {
		return xerrors.Errorf("open src failed: %w", err)
	}
	defer srcf.Close()

	dstf, err := os.Create(dst)
	if err != nil {
		return xerrors.Errorf("create dst failed: %w", err)
	}

	buf := make([]byte, 1024*1024)
	type onlyWriter struct{ io.Writer } // force buf
	type onlyReader struct{ io.Reader }
	_, err = io.CopyBuffer(onlyWriter{dstf}, onlyReader{srcf}, buf)
	if err != nil {
		dstf.Close()
		return xerrors.Errorf("copying failed: %w", err)
	}

	return dstf.Close()
}
