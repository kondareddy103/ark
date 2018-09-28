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
	"io"
	"path/filepath"

	"github.com/pkg/errors"

	arkv1api "github.com/heptio/ark/pkg/apis/ark/v1"
	"github.com/heptio/ark/pkg/util/filesystem"
)

type Reader interface {
	Extract() error

	GetResourceScope(groupResource string) (ResourceScope, bool, error)
	ListNamespaces(groupResource string) ([]string, error)
	ListContents(groupResource, namespace string) ([]string, error)
	Get(groupResource, namespace, name string) ([]byte, error)

	io.Closer
}

type ResourceScope string

const (
	ResourceScopeCluster   ResourceScope = "cluster"
	ResourceScopeNamespace ResourceScope = "namespace"

	ErrNotExtracted = "archive has not been extracted"
)

type gzipTarReader struct {
	archive io.Reader
	tempDir string
	fs      filesystem.Interface
}

func NewGzipTarReader(archive io.Reader) *gzipTarReader {
	return &gzipTarReader{
		archive: archive,

		fs: filesystem.NewFileSystem(),
	}
}

// GetResourceScope returns whether a given group/resource is cluster-scoped or
// namespace-scoped and whether the group/resource directory was found in the
// archive, or an error if there is a problem reading the extracted archive.
func (r *gzipTarReader) GetResourceScope(groupResource string) (ResourceScope, bool, error) {
	if r.tempDir == "" {
		return "", false, errors.New(ErrNotExtracted)
	}

	dir := filepath.Join(r.tempDir, arkv1api.ResourcesDir, groupResource)
	exists, err := r.fs.DirExists(dir)
	if err != nil {
		return "", false, errors.Wrapf(err, "error checking existence of directory %s", dir)
	}
	if !exists {
		return "", false, nil
	}

	clusterScopedDir := filepath.Join(dir, arkv1api.ClusterScopedDir)
	clusterScoped, err := r.fs.DirExists(clusterScopedDir)
	if err != nil {
		return "", false, errors.Wrapf(err, "error checking existence of directory %s", clusterScopedDir)
	}

	if clusterScoped {
		return ResourceScopeCluster, true, nil
	}

	return ResourceScopeNamespace, true, nil
}

func (r *gzipTarReader) ListNamespaces(groupResource string) ([]string, error) {
	if r.tempDir == "" {
		return nil, errors.New(ErrNotExtracted)
	}

	scope, found, err := r.GetResourceScope(groupResource)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.Errorf("resource %s not found in archive", groupResource)
	}
	if scope != ResourceScopeNamespace {
		return nil, errors.Errorf("resource %s is not namespace-scoped", groupResource)
	}

	dir := filepath.Join(r.tempDir, arkv1api.ResourcesDir, groupResource, arkv1api.NamespaceScopedDir)
	contents, err := r.fs.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading contents of directory %s", dir)
	}

	var namespaces []string
	for _, fi := range contents {
		if !fi.IsDir() {
			continue
		}

		namespaces = append(namespaces, fi.Name())
	}

	return namespaces, nil
}

func (r *gzipTarReader) ListContents(groupResource, namespace string) ([]string, error) {
	if r.tempDir == "" {
		return nil, errors.New(ErrNotExtracted)
	}

	var dir string
	if namespace == "" {
		dir = filepath.Join(r.tempDir, arkv1api.ResourcesDir, groupResource, arkv1api.ClusterScopedDir)
	} else {
		dir = filepath.Join(r.tempDir, arkv1api.ResourcesDir, groupResource, arkv1api.NamespaceScopedDir, namespace)
	}

	files, err := r.fs.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading directory %s", dir)
	}

	var contents []string
	for _, f := range files {
		contents = append(contents, f.Name())
	}

	return contents, nil
}

func (r *gzipTarReader) Get(groupResource, namespace, name string) ([]byte, error) {
	if r.tempDir == "" {
		return nil, errors.New(ErrNotExtracted)
	}

	var filename string
	if namespace == "" {
		filename = filepath.Join(r.tempDir, arkv1api.ResourcesDir, groupResource, arkv1api.ClusterScopedDir, name)
	} else {
		filename = filepath.Join(r.tempDir, arkv1api.ResourcesDir, groupResource, arkv1api.NamespaceScopedDir, namespace, name)
	}

	bytes, err := r.fs.ReadFile(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading file %s", filename)
	}

	return bytes, nil
}

func (r *gzipTarReader) Extract() error {
	gzr, err := gzip.NewReader(r.archive)
	if err != nil {
		return errors.Wrapf(err, "error creating gzip reader")
	}
	defer gzr.Close()
	rdr := tar.NewReader(gzr)

	dir, err := r.fs.TempDir("", "")
	if err != nil {
		return errors.Wrapf(err, "error creating temp dir")
	}

	for {
		header, err := rdr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "error getting next tar header")
		}

		target := filepath.Join(dir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			err := r.fs.MkdirAll(target, header.FileInfo().Mode())
			if err != nil {
				return errors.Wrapf(err, "error running MkdirAll for %s", target)
			}

		case tar.TypeReg:
			// make sure we have the directory created
			err := r.fs.MkdirAll(filepath.Dir(target), header.FileInfo().Mode())
			if err != nil {
				return errors.Wrapf(err, "error running MkdirAll for %s", target)
			}

			// create the file
			file, err := r.fs.Create(target)
			if err != nil {
				return errors.Wrapf(err, "error creating file %s", target)
			}
			defer file.Close()

			if _, err := io.Copy(file, rdr); err != nil {
				return errors.Wrapf(err, "error copying data to file %s", target)
			}
		}
	}

	r.tempDir = dir
	return nil
}

func (r *gzipTarReader) Close() error {
	if r.tempDir == "" {
		return nil
	}

	if err := r.fs.RemoveAll(r.tempDir); err != nil {
		return errors.Wrapf(err, "error removing temp dir %s", r.tempDir)
	}

	return nil
}
