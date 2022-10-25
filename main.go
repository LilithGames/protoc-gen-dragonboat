package main

import (
	"google.golang.org/protobuf/types/pluginpb"
	pgs "github.com/lyft/protoc-gen-star"
	pgsgo "github.com/lyft/protoc-gen-star/lang/go"
)

func main() {
	var supportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
	g := pgs.Init(pgs.SupportedFeatures(&supportedFeatures))
	g.RegisterModule(NewDragonboat())
	g.RegisterPostProcessor(pgsgo.GoFmt())
	g.Render()
}
