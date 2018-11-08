#!/bin/bash

# Copyright 2018 the Heptio Ark contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o nounset
set -o errexit
set -o pipefail

# this script copies all of the files under examples/ into a new directory,
# release-examples/ (which is gitignored so it doesn't result in the git
# state being dirty, which would prevent goreleaser from running), and then
# updates all of the image tags in those files to use $GIT_SHA (which will
# be the release/tag name).

rm -rf release-examples/ && cp -r examples/ release-examples/

find release-examples -type f -name "*.yaml" | xargs sed -i '' "s|gcr.io/heptio-images/ark:latest|gcr.io/heptio-images/ark:$GIT_SHA|g"
