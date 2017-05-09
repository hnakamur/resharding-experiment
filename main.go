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

func containsStrInArray(a []string, s string) bool {
	for _, ae := range a {
		if ae == s {
			return true
		}
	}
	return false
}

func stringSetMinus(a, b []string) []string {
	var c []string
	for _, ae := range a {
		if !containsStrInArray(b, ae) {
			c = append(c, ae)
		}
	}
	return c
}

type copyListKey struct {
	src  string
	dest string
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

	oldBuckets := buildBuckets(shardCount)
	hostnames := buildHostnames(siteCount)

	oldJump, err := buildJump(oldBuckets)
	if err != nil {
		log.Fatal(err)
	}
	oldMapping := make(map[string][]string)
	for _, h := range hostnames {
		shards := oldJump.ChooseReplicas(h, replicas)
		oldMapping[h] = shards
	}

	var newBuckets []string
	switch op {
	case "add":
		newBuckets = addBucket(oldBuckets, opShard)
	case "del":
		newBuckets = deleteBucket(oldBuckets, opShard)
		fmt.Printf("new buckets=%s\n", strings.Join(newBuckets, ","))
	default:
		log.Fatal("op must be add or del")
	}

	newJump, err := buildJump(newBuckets)
	if err != nil {
		log.Fatal(err)
	}
	newMapping := make(map[string][]string)
	for _, h := range hostnames {
		shards := newJump.ChooseReplicas(h, replicas)
		newMapping[h] = shards
	}

	copyList := make(map[copyListKey][]string)
	for _, h := range hostnames {
		oldShards := oldMapping[h]
		newShards := newMapping[h]
		delta := stringSetMinus(newShards, oldShards)
		fmt.Printf("%s\told=%s\tnew=%s\tinc=%s\n",
			h,
			strings.Join(oldShards, ","),
			strings.Join(newShards, ","),
			strings.Join(delta, ","),
		)

		for i, d := range delta {
			var srcCandidates []string
			if op == "add" {
				srcCandidates = oldShards
			} else { // "del"
				srcCandidates = stringSetMinus(oldShards, []string{opShard})
			}
			src := srcCandidates[i%len(srcCandidates)]
			key := copyListKey{src: src, dest: d}
			if hosts, ok := copyList[key]; ok {
				copyList[key] = append(hosts, h)
			} else {
				copyList[key] = []string{h}
			}
		}
	}

	for _, o := range oldBuckets {
		for _, n := range newBuckets {
			key := copyListKey{src: o, dest: n}
			if hosts, ok := copyList[key]; ok {
				fmt.Printf("copy from %s to %s\n", key.src, key.dest)
				for _, h := range hosts {
					fmt.Printf("\t%s\n", h)
				}
			}
		}
	}
}
