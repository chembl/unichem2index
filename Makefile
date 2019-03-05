
.PHONY: \
	build \
	linuxbuild

DEPLOY_PATH := build/
BIN_NAME := unichem2index

build:
	go build -o $(DEPLOY_PATH)$(BIN_NAME) main.go

linuxbuild:
	env GOOS=linux GOARCH=amd64 go build -o $(DEPLOY_PATH)$(BIN_NAME)_linux main.go

macbuild:
	env GOOS=darwin GOARCH=amd64 go build -o $(DEPLOY_PATH)$(BIN_NAME)_mac main.go

build-all: linuxbuild macbuild