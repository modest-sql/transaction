package transaction

import (
	"sync"

	"github.com/modest-sql/common"
	"github.com/rs/xid"
)

//Transaction States.
const (
	Queued = iota
	InProgress
	Done
	BatchSize = 5
)

//TransactionLock is the global transaction mutex
var TransactionLock *sync.Mutex

//Transaction instance definition.
type Transaction struct {
	TransactionID         xid.ID        `json:"Transaction_ID"`
	TransactionQueries    []string      `json:"TransactionQueries"`
	CommandsInTransaction []interface{} `json:"TransactionCommands"`
	TransactionState      int           `json:"Transaction_State"`
}

//NewTransaction creates an instance of Transaction.
func NewTransaction(Commands []interface{}) Transaction {
	T := Transaction{
		xid.New(),
		make([]string, len(Commands)),
		Commands,
		0,
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

//ExcecuteTransaction excectes the commands inside a transaction.
func (T *Transaction) ExcecuteTransaction() {
	T.TransactionState = 1
	TransactionLock.Lock()
	for subindex := 0; subindex < len(T.CommandsInTransaction); subindex++ {
		switch v := T.CommandsInTransaction[subindex].(type) {
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
	TransactionLock.Unlock()
	T.TransactionState = 2
}

//Manager instance definition.
type Manager struct {
	TransactionQueue []Transaction `json:"TransactionQueue"`
	ExcecutionBatch  []Transaction `json:"ExcecutionBatch"`
}

//NewTransactionManager creates an instance of TransactionManger.
func NewTransactionManager() Manager {
	return Manager{
		make([]Transaction, 0),
		make([]Transaction, 0),
	}
}

//PopTransactionQueue pops the first transaction in the queue.
func (TM *Manager) PopTransactionQueue() {
	TM.TransactionQueue = TM.TransactionQueue[1:]
}

//AddTransactionToQueue adds a new Transaction to the TransactionManager TransactionQueue.
func (TM *Manager) AddTransactionToQueue(Commands []interface{}) {
	T := NewTransaction(Commands)

	TM.TransactionQueue = append(TM.TransactionQueue, T)
}

/*PrepareExcecutionBatch adds BATCH_SIZE amount of Transactions from the TransactionManager
TransactionQueue into the ExcecutionQueue for them to be excecuted.*/
func (TM *Manager) PrepareExcecutionBatch() {
	if len(TM.TransactionQueue) > BatchSize {
		for index := 0; index < BatchSize; index++ {
			TM.ExcecutionBatch[index] = TM.TransactionQueue[index]
		}
		for index := 0; index < BatchSize; index++ {
			TM.PopTransactionQueue()
		}

	} else {
		for index := 0; len(TM.TransactionQueue) != 0; index++ {
			TM.ExcecutionBatch[index] = TM.TransactionQueue[index]
			TM.PopTransactionQueue()
		}
	}
}

//ExcecuteBatch Transactions are executed serialized.
func (TM *Manager) ExcecuteBatch() {
	for index := 0; index < BatchSize; index++ {
		go TM.ExcecutionBatch[index].ExcecuteTransaction()
	}
}
