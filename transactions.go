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

//transactionLock is the global transaction mutex
var transactionLock sync.Mutex

//TChannel is used to communicate with StartTransactionManager
var TChannel = make(chan transaction)

//TRChannel is used to deliver the result of the treansactions.
var TRChannel = make(chan []interface{})

//TEChannel is used to deliver the errors, if any, of the transactions.
var TEChannel = make(chan []error)

//transaction instance definition.
type transaction struct {
	TransactionID         xid.ID   `json:"Transaction_ID"`
	TransactionQueries    []string `json:"TransactionQueries"`
	commandsInTransaction []interface{}
	TransactionState      int `json:"Transaction_State"`
}

//newTransaction creates an instance of Transaction.
func newTransaction(Commands []interface{}) transaction {
	T := transaction{
		xid.New(),
		make([]string, len(Commands)),
		Commands,
		Queued,
	}

	T.parseCommandsToQueries()

	return T
}

/*parseCommandsToQueries interprets the commands in a Transaction and
stores a queries relative to those commands.*/
func (T *transaction) parseCommandsToQueries() {
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

//excecuteTransaction excectes the commands inside a transaction.
func (T *transaction) excecuteTransaction(DB *data.Database) {
	transactionLock.Lock()
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

	transactionLock.Unlock()
	T.TransactionState = Done
}

//manager instance definition.
type manager struct {
	TransactionQueue []transaction `json:"TransactionQueue"`
}

//newTransactionManager creates an instance of TransactionManger.
func newTransactionManager() manager {
	return manager{
		make([]transaction, 0),
	}
}

//GetTransactions returns a Transaction array containing both, Transactions in queue and in excution batch.
func (TM *manager) GetTransactions() []transaction {
	Transactions := make([]transaction, 0)

	for index := 0; index < len(TM.TransactionQueue); index++ {
		Transactions = append(Transactions, TM.TransactionQueue[index])
	}

	return Transactions
}

//popTransactionQueue pops the first transaction in the queue.
func (TM *manager) popTransactionQueue() {
	TM.TransactionQueue = TM.TransactionQueue[1:]
}

//addTransactionToQueue adds a new Transaction to the TransactionManager TransactionQueue.
func (TM *manager) addTransactionToQueue(t transaction) {
	TM.TransactionQueue = append(TM.TransactionQueue, t)
}

//AddTransactionToManager sends Transactions to Manager through a channel.
func AddTransactionToManager(t transaction) {
	TChannel <- t
}

/*StartTransactionManager is ment to be called once by the engine, afterwards it will receive incoming
transactions and will add them to its queue and then will excecute them by moving it into the excecution
batch.*/
func StartTransactionManager(DB *data.Database) {
	//TransactionManager := NewTransactionManager()
	for {
		transaction := <-TChannel
		go transaction.excecuteTransaction(DB)

		transactionResults := <-TRChannel
		fmt.Println(transactionResults)
		transactionErrors := <-TEChannel
		fmt.Println(transactionErrors)
	}
}
