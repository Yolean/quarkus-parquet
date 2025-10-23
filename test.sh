#!/usr/bin/env bash
set -euo pipefail

mvn install -Dquarkus.native.container-build=false -Dmaven.repo.local=.m2/repo
cd integration-tests
mvn test -Pnative -Dquarkus.native.container-build=false -Dmaven.repo.local=../.m2/repo
