// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package main

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
)

func (s *CabinetTest) doSequenceCreate(sType string, sNode string) (*pb.Sequential, error){
	return s.client.SequentialCreate(s.ctx, &pb.Sequential{Type: sType, Node: sNode})
}

func (s *CabinetTest) doSequenceUpdate(sType string, sNode string, sID uint32) (*pb.MutationResponse, error){
	return s.client.SequentialUpdate(s.ctx, &pb.Sequential{Type: sType, Node: sNode, Seqid: sID})
}

func (s *CabinetTest) doSequenceDelete(sType string, sID uint32) (*pb.MutationResponse, error){
	return s.client.SequentialDelete(s.ctx, &pb.Sequential{Type: sType, Seqid: sID})
}

func (s *CabinetTest) doSequenceGet(sType string, sID uint32) (*pb.Sequential, error){
	return s.client.SequentialGet(s.ctx, &pb.Sequential{Type: sType, Seqid: sID})
}

