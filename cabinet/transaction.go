// Package: net.ikigai.cds
// Module: cabinet.services.test
//
// Author: Narcis M. PAP
// Copyright (c) 2018 Ikigai Cloud. All rights reserved.

package cabinet

import (
	pb "cds.ikigai.net/cabinet.v1/rpc"
	"context"
	"fmt"
	"io"
	"sync"
)

const (
	TRANSACTION_ERROR_CONN     = 0
	TRANSACTION_ERROR_CLOSING  = 1
	TRANSACTION_ERROR_SENDING  = 2
	TRANSACTION_ERROR_RESPONSE = 3

	TRANSACTION_ERROR_OPERATION = 10
	TRANSACTION_ERROR_EMPTY     = 11
)

type TransactionError struct {
	msg   string
	class int
}

func (e *TransactionError) Error() string {
	return fmt.Sprintf("ERR(%d): %s", e.class, e.msg)
}

type Transaction struct {
	actions   map[uint32]*pb.TransactionAction
	response  map[uint32]*pb.TransactionActionResponse
	actionIDs []uint32
	resMux    sync.Mutex
	actPos    uint32

	queueErr []error

	idMap  map[string]string
	tmpMap map[uint32]string
	mapMux sync.Mutex

	resError    error
	resErrorMux sync.Mutex

	client pb.CDSCabinetClient
	ctx    context.Context
}

func (c *Transaction) Setup(ctx context.Context, cli pb.CDSCabinetClient) {
	c.actions = make(map[uint32]*pb.TransactionAction)
	c.response = make(map[uint32]*pb.TransactionActionResponse)
	c.queueErr = make([]error, 0)
	c.actionIDs = make([]uint32, 0)

	c.idMap = make(map[string]string)
	c.tmpMap = make(map[uint32]string)
	c.actPos = uint32(1)

	c.client = cli
	c.ctx = ctx

	c.resError = nil
}

func (c *Transaction) Operation(o pb.TransactionAction) {
	if _, inActions := c.actions[o.ActionId]; inActions {
		c.queueErr = append(c.queueErr, &TransactionError{msg: "duplicate actionID", class: TRANSACTION_ERROR_OPERATION})
		return
	}

	c.actions[o.ActionId] = &o
	c.actionIDs = append(c.actionIDs, o.ActionId)
}

func (c *Transaction) O(o *pb.TransactionAction) {
	o.ActionId = c.actPos
	c.actPos += 1

	c.Operation(*o)
}

func (c *Transaction) Pos() uint32 {
	return c.actPos
}

func (c *Transaction) GetIdMap() map[string]string {
	return c.idMap
}

func (c *Transaction) Commit() error {
	if len(c.actions) == 0 {
		return &TransactionError{msg: "no queued transactions", class: TRANSACTION_ERROR_EMPTY}
	} else if len(c.queueErr) > 0 {
		for er := range c.queueErr {
			return c.queueErr[er]
		}
	}

	stream, err := c.client.Transaction(c.ctx)

	if err != nil {
		return &TransactionError{msg: fmt.Sprintf("connection error: %s", err), class: TRANSACTION_ERROR_CONN}
	}

	wc := make(chan struct{})

	go func() {
		for {
			actionResponse, err := stream.Recv()
			// fmt.Printf("T.(receive) = %v, %v\n", actionResponse, err)

			if err == io.EOF {
				close(wc)
				return
			} else if err != nil {
				c.resErrorMux.Lock()
				c.resError = &TransactionError{msg: fmt.Sprintf("%s", err), class: TRANSACTION_ERROR_RESPONSE}
				c.resErrorMux.Unlock()

				close(wc)
				return
			} else {
				c.resMux.Lock()
				c.response[actionResponse.ActionId] = actionResponse
				c.resMux.Unlock()

				switch tReq := actionResponse.Response.(type) {
				case *pb.TransactionActionResponse_NodeCreate:
					c.mapMux.Lock()
					c.idMap[c.tmpMap[actionResponse.ActionId]] = tReq.NodeCreate.Id
					c.mapMux.Unlock()
				}
			}
		}
	}()

	for _, aID := range c.actionIDs {
		// fmt.Printf("T.(send) %v\n", c.actions[aID])

		if err := stream.Send(c.actions[aID]); err != nil {
			return &TransactionError{msg: fmt.Sprintf("sending error: %s", err), class: TRANSACTION_ERROR_SENDING}
		}

		switch tReq := c.actions[aID].Action.(type) {
		case *pb.TransactionAction_NodeCreate:
			c.mapMux.Lock()
			c.tmpMap[c.actions[aID].ActionId] = tReq.NodeCreate.Id
			c.mapMux.Unlock()
		}
	}

	err = stream.CloseSend()

	if err != nil {
		return &TransactionError{msg: fmt.Sprintf("close conn error: %s", err), class: TRANSACTION_ERROR_CLOSING}
	}

	<-wc

	return c.resError
}
