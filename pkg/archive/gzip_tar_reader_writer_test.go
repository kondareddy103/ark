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
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/heptio/ark/pkg/util/test"
)

func TestReadingAndWriting(t *testing.T) {
	var (
		buf        = bytes.NewBuffer([]byte{})
		writer     = NewGzipTarWriter(buf)
		items      = []string{"bar", "foo"}
		namespaces = []string{"ns-1", "ns-2"}
		resources  = map[string]ResourceScope{
			"pods":              ResourceScopeNamespace,
			"replicasets":       ResourceScopeNamespace,
			"namespaces":        ResourceScopeCluster,
			"persistentvolumes": ResourceScopeCluster,
		}
	)

	// test writer
	for resource, scope := range resources {
		switch scope {
		case ResourceScopeCluster:
			for _, item := range items {
				content := fmt.Sprintf("%s-%s-content", resource, item)
				require.NoError(t, writer.Write(resource, "", item, content))
			}
		case ResourceScopeNamespace:
			for _, ns := range namespaces {
				for _, item := range items {
					content := fmt.Sprintf("%s-%s-%s-content", resource, ns, item)
					require.NoError(t, writer.Write(resource, ns, item, content))
				}
			}
		}
	}

	require.NoError(t, writer.Close())

	assert.NotZero(t, buf.Len())

	// test reader
	reader := NewGzipTarReader(buf)
	reader.fs = test.NewFakeFileSystem()
	defer require.NoError(t, reader.Close())

	// items come out of the archive with a .json suffix
	items = []string{"bar.json", "foo.json"}

	require.NoError(t, reader.Extract())

	for resource, expectedScope := range resources {
		scope, found, err := reader.GetResourceScope(resource)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, expectedScope, scope)

		switch scope {
		case ResourceScopeCluster:
			_, err := reader.ListNamespaces(resource)
			assert.NotNil(t, err)

			contents, err := reader.ListContents(resource, "")
			require.NoError(t, err)
			sort.Strings(contents)
			assert.Equal(t, items, contents)

			for _, item := range contents {
				content, err := reader.Get(resource, "", item)
				require.NoError(t, err)

				// note that the content of the item is expected to be quoted, because
				// it's marshalled/unmarshalled to/from JSON, which quotes string literals.
				assert.Equal(t, fmt.Sprintf("\"%s-%s-content\"", resource, strings.TrimSuffix(item, ".json")), string(content))
			}
		case ResourceScopeNamespace:
			res, err := reader.ListNamespaces(resource)
			require.NoError(t, err)
			sort.Strings(res)
			assert.Equal(t, namespaces, res)

			for _, ns := range namespaces {
				contents, err := reader.ListContents(resource, ns)
				require.NoError(t, err)
				sort.Strings(contents)
				assert.Equal(t, items, contents)

				for _, item := range contents {
					content, err := reader.Get(resource, ns, item)
					require.NoError(t, err)

					// note that the content of the item is expected to be quoted, because
					// it's marshalled/unmarshalled to/from JSON, which quotes string literals.
					assert.Equal(t, fmt.Sprintf("\"%s-%s-%s-content\"", resource, ns, strings.TrimSuffix(item, ".json")), string(content))
				}
			}
		}
	}

	// negative test cases
	_, found, err := reader.GetResourceScope("nonexistent")
	assert.Nil(t, err)
	assert.False(t, found)

	_, err = reader.ListContents("nonexistent", "ns-1")
	assert.NotNil(t, err)

	_, err = reader.ListContents("pods", "nonexistent")
	assert.NotNil(t, err)

	_, err = reader.Get("nonexistent", "ns-1", "bar.json")
	assert.NotNil(t, err)

	_, err = reader.Get("pods", "nonexistent", "bar.json")
	assert.NotNil(t, err)

	_, err = reader.Get("pods", "ns-1", "nonexistent.json")
	assert.NotNil(t, err)
}
