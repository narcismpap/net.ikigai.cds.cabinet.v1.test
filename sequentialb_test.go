// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"testing"
)

func BenchmarkSequentialCreate(b *testing.B) {
	tester := CabinetTest{bench: b}
	tester.setup()

	for n := 0; n < b.N; n++ {
		_, _ = tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Node: "XXXXX"})
	}

	tester.tearDown()
}

func BenchmarkSequentialGet(b *testing.B) {
	tester := CabinetTest{bench: b}
	tester.setup()

	sq, err := tester.client.SequentialCreate(tester.ctx, &pb.Sequential{Type: "n", Node: "XXXXX"})

	if err != nil{
		b.Fatalf("Unable to setup: %v", err)
	}

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		_, _ = tester.client.SequentialGet(tester.ctx, &pb.Sequential{Type: "n", Seqid: sq.GetSeqid()})
	}

	tester.tearDown()
}
