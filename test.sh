#!/usr/bin/env bash
set -euo pipefail
set -x

M2_REPO="$(pwd)/.m2/repo"
echo "Using local maven repo at $M2_REPO"

mvn -pl deployment -am clean install \
  -Dmaven.repo.local="$M2_REPO"

mvn -pl integration-tests -am verify \
  -Dmaven.repo.local="$M2_REPO"

mvn -pl integration-tests -am verify \
  -Dmaven.repo.local="$M2_REPO" \
  -Dnative -Dquarkus.native.container-build=false
