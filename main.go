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

func main() {
	var shardCount int
	flag.IntVar(&shardCount, "shard-count", 3, "shard count")
	var siteCount int
	flag.IntVar(&siteCount, "site-count", 10, "site count")
	var replicas int
	flag.IntVar(&replicas, "replicas", 2, "replica count")
	flag.Parse()

	buckets := buildBuckets(shardCount)

	jmp, err := buildJump(buckets)
	if err != nil {
		log.Fatal(err)
	}

	countPerShard := make(map[string]int)
	for _, b := range buckets {
		countPerShard[b] = 0
	}

	for i := 0; i < siteCount; i++ {
		hostname := fmt.Sprintf("%d.example.jp", i)
		shards := jmp.ChooseReplicas(hostname, 2)
		for _, s := range shards {
			countPerShard[s]++
		}
		log.Printf("hostname=%s, shards=%s", hostname, strings.Join(shards, ","))
	}
	for _, b := range buckets {
		log.Printf("shard=%s, count=%d", b, countPerShard[b])
	}
}
