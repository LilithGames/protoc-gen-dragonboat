# protoc-gen-dragonboat

Protobuf gen for [dragonboat](https://github.com/lni/dragonboat) statemachine.

For usage see example at `testdata/test.proto` and `dragonboat_test.go`.

## Install

```
go install github.com/LilithGames/protoc-gen-dragonboat@latest

protoc -I=. --go_out=paths=source_relative:. --dragonboat_out=paths=source_relative:. testdata/test.proto
```
