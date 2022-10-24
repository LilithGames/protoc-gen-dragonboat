.PHONY: proto
proto:
	@protoc -I=. --go_out=paths=source_relative:. runtime/dragonboat.proto

.PHONY: build
build: proto
	@go build github.com/LilithGames/protoc-gen-dragonboat

.PHONY: test
test: build
	@protoc -I=. --go_out=paths=source_relative:. --dragonboat_out=paths=source_relative:. testdata/test.proto
