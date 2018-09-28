/*
Copyright 2018 the Heptio Ark contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package archive

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"io"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
)

type Writer interface {
	Write(groupResource, namespace, name string, item interface{}) error
	io.Closer
}

type gzipTarWriter struct {
	base io.Writer
	gzip *gzip.Writer
	tar  *tar.Writer
}

func NewGzipTarWriter(writer io.Writer) *gzipTarWriter {
	var (
		gzipWriter = gzip.NewWriter(writer)
		tarWriter  = tar.NewWriter(gzipWriter)
	)

	return &gzipTarWriter{
		base: writer,
		gzip: gzipWriter,
		tar:  tarWriter,
	}
}

func (w *gzipTarWriter) Write(groupResource, namespace, name string, item interface{}) error {
	var filePath string
	if namespace != "" {
		filePath = filepath.Join(arkv1api.ResourcesDir, groupResource, arkv1api.NamespaceScopedDir, namespace, name+".json")
	} else {
		filePath = filepath.Join(arkv1api.ResourcesDir, groupResource, arkv1api.ClusterScopedDir, name+".json")
	}

	itemBytes, err := json.Marshal(item)
	if err != nil {
		return errors.WithStack(err)
	}

	hdr := &tar.Header{
		Name:     filePath,
		Size:     int64(len(itemBytes)),
		Typeflag: tar.TypeReg,
		Mode:     0755,
		ModTime:  time.Now(),
	}

	if err := w.tar.WriteHeader(hdr); err != nil {
		return errors.WithStack(err)
	}

	if _, err := w.tar.Write(itemBytes); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (w *gzipTarWriter) Close() error {
	// TODO handle errors
	w.tar.Close()
	w.gzip.Close()

	return nil
}
