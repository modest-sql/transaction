package main

import (
	"container/list"
	"log"

	"github.com/rs/xid"
)

//Transaction States.
// 1 -> QUEUED
// 2 -> IN PROGRESS
// 3 -> DONE

/* Commit, Fail, Rollback(Marked) Pending. */

type TransactionSystem struct {
	Transactions             list.List
	Historic_Transaction_Log log.Logger
}

func (T TransactionSystem) AddTransactionToQueue(query string) {
	NewTransaction := Transaction{
		genXid(),
		query,
		1,
	}
	T.Transactions.PushBack(NewTransaction)
}

type Transaction struct {
	TransactionID    xid.ID
	TransactionQuery string
	TransactionState int
}

func (T Transaction) DisplayTransaction() {

}

func genXid() xid.ID {
	ID := xid.New()
	return ID
}
