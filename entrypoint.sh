#!/bin/bash

go install ./...
ponzu new --dev /path
cd $GOPATH/src/path && ponzu build
ponzu run --bind 0.0.0.0

