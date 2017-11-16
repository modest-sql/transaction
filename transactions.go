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
	TransactionID         xid.ID   `json:"Transaction_ID"`
	TransactionQueries    []string `json:"TransactionQueries"`
	commandsInTransaction []interface{}
	TransactionState      int `json:"Transaction_State"`
}

//NewTransaction creates an instance of Transaction.
func NewTransaction(Commands []interface{}) Transaction {
	T := Transaction{
		xid.New(),
		make([]string, len(Commands)),
		Commands,
		Queued,
	}

	T.ParseCommandsToQueries()

	return T
}

/*ParseCommandsToQueries interprets the commands in a Transaction and
stores a queries relative to those commands.*/
func (T *Transaction) ParseCommandsToQueries() {
	for index := 0; index < len(T.commandsInTransaction); index++ {
		switch v := T.commandsInTransaction[index].(type) {
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
	TransactionLock.Lock()
	T.TransactionState = InProgress
	for subindex := 0; subindex < len(T.commandsInTransaction); subindex++ {
		switch v := T.commandsInTransaction[subindex].(type) {
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
	T.TransactionState = Done
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

//GetTransactions returns a Transaction array containing both, Transactions in queue and in excution batch.
func (TM *Manager) GetTransactions() []Transaction {
	Transactions := make([]Transaction, 0)

	for index := 0; index < len(TM.ExcecutionBatch); index++ {
		Transactions = append(Transactions, TM.ExcecutionBatch[index])
	}

	for index := 0; index < len(TM.TransactionQueue); index++ {
		Transactions = append(Transactions, TM.TransactionQueue[index])
	}

	return Transactions
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
