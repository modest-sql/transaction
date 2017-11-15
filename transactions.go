package transaction

import (
	"sync"

	"github.com/modest-sql/common"
	"github.com/rs/xid"
)

//Transaction States.
// 1 -> QUEUED
// 2 -> IN PROGRESS
// 3 -> DONE

//Batch size defines the amount of transactions to be scheduled at once.
var BATCH_SIZE = 5

type Transaction struct {
	TransactionID         xid.ID        `json:"Transaction_ID"`
	TransactionQueries    []string      `json:"TransactionQueries"`
	CommandsInTransaction []interface{} `json:"TransactionCommands"`
	TransactionState      int           `json:"Transaction_State"`
}

//NewTransaction creates an instance of Transaction.
func NewTransaction(Commands []interface{}) Transaction {
	T := Transaction{
		GenerateXID(),
		make([]string, len(Commands)),
		Commands,
		1,
	}

	T.ParseCommandsToQueries()

	return T
}

/*ParseCommandsToQueries interprets the commands in a Transaction and
stores a queries relative to those commands.*/
func (T *Transaction) ParseCommandsToQueries() {
	for index := 0; index < len(T.CommandsInTransaction); index++ {
		switch v := T.CommandsInTransaction[index].(type) {
		case *common.CreateTableCommand:
			CTC := common.CreateTableCommand(*v)
			T.TransactionQueries[index] = "CREATE TABLE " + CTC.TableName() + "."

		case *common.SelectTableCommand:
			STC := common.SelectTableCommand(*v)
			T.TransactionQueries[index] = "SELECT * FROM TABLE " + STC.SourceTableName() + "."

		default:
			T.TransactionQueries[index] = "UNKNOWN QUERY."
			_ = v
		}
	}
}

//GenerateXID function generates unique ID's.
func GenerateXID() xid.ID {
	ID := xid.New()
	return ID
}

type TransactionManager struct {
	TransactionLock  *sync.Mutex
	TransactionQueue []Transaction `json:"TransactionQueue"`
	ExcecutionBatch  []Transaction `json:"ExcecutionBatch"`
}

//NewTransactionManager creates an instance of TransactionManger.
func NewTransactionManager() TransactionManager {
	return TransactionManager{
		&sync.Mutex{},
		make([]Transaction, 0),
		make([]Transaction, 0),
	}
}

//AddTransactionToQueue adds a new Transaction to the TransactionManager TransactionQueue.
func (TM *TransactionManager) AddTransactionToQueue(Commands []interface{}) {
	T := NewTransaction(Commands)

	TM.TransactionQueue = append(TM.TransactionQueue, T)
}

/*PrepareExcecutionBatch adds BATCH_SIZE amount of Transactions from the TransactionManager
TransactionQueue into the ExcecutionQueue for them to be excecuted.*/
func (TM *TransactionManager) PrepareExcecutionBatch() {
	if len(TM.TransactionQueue) > BATCH_SIZE {
		for index := 0; index < BATCH_SIZE; index++ {
			TM.ExcecutionBatch[index] = TM.TransactionQueue[index]
		}
	} else {
		for index := 0; index < len(TM.TransactionQueue); index++ {
			TM.ExcecutionBatch[index] = TM.TransactionQueue[index]
		}
	}
}

//Transactions are executed serialized.
func (TM *TransactionManager) ExcecuteBatch() {
	for index := 0; index < BATCH_SIZE; index++ {
		TM.TransactionLock.Lock()
		TM.ExcecutionBatch[index].TransactionState = 2
		for subindex := 0; subindex < len(TM.ExcecutionBatch[index].CommandsInTransaction); subindex++ {
			switch v := TM.ExcecutionBatch[index].CommandsInTransaction[index].(type) {
			case *common.CreateTableCommand:
				CTC := common.CreateTableCommand(*v)
				CTC.Excecute()

			case *common.SelectTableCommand:
				STC := common.SelectTableCommand(*v)
				STC.Excecute()

			default:
				_ = v
			}
		}
		TM.TransactionLock.Unlock()
		TM.ExcecutionBatch[index].TransactionState = 3
	}
}
