package transaction

import (
	"fmt"
	"sync"

	"github.com/modest-sql/data"

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
var TransactionLock sync.Mutex

//TChannel is used to communicate with StartTransactionManager
var TChannel = make(chan Transaction)

//TRChannel is used to deliver the result of the treansactions.
var TRChannel = make(chan []interface{})

//TEChannel is used to deliver the errors, if any, of the transactions.
var TEChannel = make(chan []error)

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
			T.TransactionQueries[index] = "SELECT * FROM TABLE " + STC.SourceTable() + "."

		default:
			T.TransactionQueries[index] = "UNKNOWN QUERY."
			_ = v
		}
	}
}

//ExcecuteTransaction excectes the commands inside a transaction.
func (T *Transaction) ExcecuteTransaction(DB *data.Database) {
	TransactionLock.Lock()
	T.TransactionState = InProgress

	ExcecutionResults := make([]interface{}, 0)
	ExcecutionErrors := make([]error, 0)

	for index := 0; index < len(T.commandsInTransaction); index++ {
		ExcecutionResult, ExcecutionError := DB.ExecuteCommand(T.commandsInTransaction[index])
		ExcecutionResults = append(ExcecutionResults, ExcecutionResult)
		ExcecutionErrors = append(ExcecutionErrors, ExcecutionError)
	}

	//fmt.Println(ExcecutionResults[0])

	TRChannel <- ExcecutionResults
	//TEChannel <- ExcecutionErrors

	TransactionLock.Unlock()
	T.TransactionState = Done
}

//Manager instance definition.
type Manager struct {
	TransactionQueue []Transaction `json:"TransactionQueue"`
}

//NewTransactionManager creates an instance of TransactionManger.
func NewTransactionManager() Manager {
	return Manager{
		make([]Transaction, 0),
	}
}

//GetTransactions returns a Transaction array containing both, Transactions in queue and in excution batch.
func (TM *Manager) GetTransactions() []Transaction {
	Transactions := make([]Transaction, 0)

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
func (TM *Manager) AddTransactionToQueue(t Transaction) {
	TM.TransactionQueue = append(TM.TransactionQueue, t)
}

//AddTransactionToManager sends Transactions to Manager through a channel.
func AddTransactionToManager(t Transaction) {
	TChannel <- t
}

/*StartTransactionManager is ment to be called once by the engine, afterwards it will receive incoming
transactions and will add them to its queue and then will excecute them by moving it into the excecution
batch.*/
func StartTransactionManager(DB *data.Database) {
	//TransactionManager := NewTransactionManager()
	for {
		transaction := <-TChannel
		go transaction.ExcecuteTransaction(DB)

		transactionResults := <-TRChannel
		fmt.Println(transactionResults)
		transactionErrors := <-TEChannel
		fmt.Println(transactionErrors)
	}
}
