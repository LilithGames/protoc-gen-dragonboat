package main

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"log"
	"strconv"

	pb "github.com/LilithGames/protoc-gen-dragonboat/testdata"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type node struct {
	name  string
	state map[string]any
}

func newNode(name string) *node {
	return &node{
		name:  name,
		state: make(map[string]any, 0),
	}
}
func (it *node) Value(name string) any {
	if value, ok := it.state[name]; ok {
		return value
	}
	return nil
}
func (it *node) SetValue(v any) {
	it.state[it.name] = v
}
func (it *node) NodeMeta(limit int) []byte {
	return nil
}
func (it *node) NotifyMsg([]byte) {}
func (it *node) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}
func (it *node) LocalState(join bool) []byte {
	bs, err := json.Marshal(it.state)
	if err != nil {
		panic(err)
	}
	return bs
}
func (it *node) MergeRemoteState(buf []byte, join bool) {
	var rstate map[string]any
	err := json.Unmarshal(buf, &rstate)
	if err != nil {
		panic(err)
	}
	rstate[it.name] = it.state[it.name]
	it.state = rstate
}

func newMemberlist(name string, initials map[string]string) (*memberlist.Memberlist, *node) {
	host, port, err := net.SplitHostPort(initials[name])
	if err != nil {
		panic(err)
	}
	iport, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		panic(err)
	}
	conf := memberlist.DefaultLANConfig()
	conf.Name = name
	node := newNode(name)
	conf.Delegate = node
	conf.BindAddr = host
	conf.BindPort = int(iport)
	ml, err := memberlist.Create(conf)
	if err != nil {
		panic(err)
	}
	members := make([]string, 0, len(initials))
	for _, addr := range initials {
		members = append(members, addr)
	}
	if _, err := ml.Join(members); err != nil {
		panic(err)
	}
	return ml, node
}

func pushpull(ml *memberlist.Memberlist, initials map[string]string) {
	members := make([]string, 0, len(initials))
	for _, addr := range initials {
		members = append(members, addr)
	}
	ml.Join(members)
}

func TestMemberlist(t *testing.T) {
	initials := map[string]string{
		"node1": "127.0.0.1:3201",
		"node2": "127.0.0.1:3202",
		"node3": "127.0.0.1:3203",
	}
	ml := make(map[string]*memberlist.Memberlist, len(initials))
	nodes := make(map[string]*node, len(initials))
	for name := range initials {
		ml[name], nodes[name] = newMemberlist(name, initials)
	}
	nodes["node1"].SetValue("hello1")
	pushpull(ml["node1"], initials)
	log.Println("[INFO]", fmt.Sprintf("pushpull done"))
	v := nodes["node2"].Value("node1")
	fmt.Printf("get node1 from node2: %+v\n", v)
}
func TestMetaLimit(t *testing.T) {
	meta := pb.Meta{NodeIds: make(map[uint64]uint64)}
	for i := uint64(0); i < 128; i++ {
		meta.NodeIds[i] = i
	}
	bs, err := proto.Marshal(&meta)
	assert.Nil(t, err)
	println(len(bs))
}
