package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/dgryski/go-farm"
	"github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/pkg/errors"
)

func buildHostnames(siteCount int) []string {
	var hostnames []string
	for i := 0; i < siteCount; i++ {
		hostnames = append(hostnames, fmt.Sprintf("%d.example.jp", i))
	}
	return hostnames
}

func buildBuckets(shardCount int) []string {
	var buckets []string
	for i := 0; i < shardCount; i++ {
		buckets = append(buckets, fmt.Sprintf("met%d", i+1))
	}
	return buckets
}

func buildJump(buckets []string) (*jump.Jump, error) {
	jmp := jump.New(farm.Hash64)
	err := jmp.SetBuckets(buckets)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return jmp, nil
}

func deleteBucket(buckets []string, bucket string) []string {
	for i, b := range buckets {
		if b == bucket {
			return append(buckets[:i], buckets[i+1:]...)
		}
	}
	return buckets
}

func addBucket(buckets []string, bucket string) []string {
	return append(buckets, bucket)
}

func main() {
	var shardCount int
	flag.IntVar(&shardCount, "shard-count", 3, "shard count")
	var siteCount int
	flag.IntVar(&siteCount, "site-count", 10, "site count")
	var replicas int
	flag.IntVar(&replicas, "replicas", 2, "replica count")
	var op string
	flag.StringVar(&op, "op", "del", "operation: add or del")
	var opShard string
	flag.StringVar(&opShard, "op-shard", "", "operation target shard")
	flag.Parse()

	buckets := buildBuckets(shardCount)
	hostnames := buildHostnames(siteCount)

	oldJump, err := buildJump(buckets)
	if err != nil {
		log.Fatal(err)
	}
	oldMapping := make(map[string][]string)
	for _, h := range hostnames {
		shards := oldJump.ChooseReplicas(h, replicas)
		oldMapping[h] = shards
	}

	switch op {
	case "add":
		buckets = addBucket(buckets, opShard)
	case "del":
		buckets = deleteBucket(buckets, opShard)
		fmt.Printf("new buckets=%s\n", strings.Join(buckets, ","))
	default:
		log.Fatal("op must be add or del")
	}

	newJump, err := buildJump(buckets)
	if err != nil {
		log.Fatal(err)
	}
	newMapping := make(map[string][]string)
	for _, h := range hostnames {
		shards := newJump.ChooseReplicas(h, replicas)
		newMapping[h] = shards
	}

	for _, h := range hostnames {
		fmt.Printf("%s old=%s new=%s\n", h, strings.Join(oldMapping[h], ","), strings.Join(newMapping[h], ","))
	}
}
