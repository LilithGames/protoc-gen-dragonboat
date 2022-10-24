package main

import (
	"text/template"

	// "github.com/golang/protobuf/proto"
	pgs "github.com/lyft/protoc-gen-star"
	pgsgo "github.com/lyft/protoc-gen-star/lang/go"
)

type module struct {
	*pgs.ModuleBase
	ctx pgsgo.Context
	tpl *template.Template
}

func NewDragonboat() pgs.Module {
	return &module{ModuleBase: &pgs.ModuleBase{}}
}

func (it *module) InitContext(c pgs.BuildContext) {
	it.ModuleBase.InitContext(c)
	it.ctx = pgsgo.InitContext(c.Parameters())
	tpl := template.New("event").Funcs(map[string]interface{}{
		"package": it.ctx.PackageName,
	})
	it.tpl = template.Must(tpl.Parse(tmpl))
}

func (it *module) Name() string {
	return "dragonboat"
}

func (it *module) Execute(targets map[string]pgs.File, pkgs map[string]pgs.Package) []pgs.Artifact {
	for _, f := range targets {
		it.generate(f)
	}
	return it.Artifacts()
}

func (it *module) generate(f pgs.File) {
	name := it.ctx.OutputPath(f).SetExt(".dragonboat.go").String()
	it.AddGeneratorTemplateFile(name, it.tpl, f)
}

