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
)

//transactionLock is the global transaction mutex
var transactionLock sync.Mutex
var transactionQueriesLock sync.Mutex

//TChannel is used to communicate with StartTransactionManager
var tChannel = make(chan Transaction)

var tManager manager

//Transaction instance definition.
type Transaction struct {
	TransactionID         xid.ID   `json:"Transaction_ID"`
	TransactionQueries    []string `json:"TransactionQueries"`
	CommandsInTransaction []common.Command
	TransactionState      int `json:"Transaction_State"`
}

//newTransaction creates an instance of Transaction.
func newTransaction(commands []common.Command) Transaction {
	T := Transaction{
		TransactionID:         xid.New(),
		CommandsInTransaction: commands,
		TransactionState:      Queued,
	}

	for _, command := range commands {
		T.TransactionQueries = append(T.TransactionQueries, command.String())
	}

	return T
}

//excecuteTransaction excectes the commands inside a transaction.
func (T *Transaction) excecuteTransaction() {
	transactionLock.Lock()
	T.TransactionState = InProgress

	for _, command := range T.CommandsInTransaction {
		command.Instruction()
	}

	transactionLock.Unlock()
	T.TransactionState = Done
}

//manager instance definition.
type manager struct {
	TransactionQueue []Transaction `json:"TransactionQueue"`
}

//GetTransactions returns the array of transactions in memory.
func GetTransactions() []Transaction {
	Transactions := make([]Transaction, 0)

	transactionQueriesLock.Lock()
	for _, transaction := range tManager.TransactionQueue {
		Transactions = append(Transactions, transaction)
	}
	transactionQueriesLock.Unlock()

	return Transactions
}

//popTransactionQueue pops the first transaction in the queue.
func popTransactionQueue() {
	transactionQueriesLock.Lock()
	tManager.TransactionQueue = tManager.TransactionQueue[1:]
	transactionQueriesLock.Unlock()
}

//addTransactionToQueue adds a new Transaction to the TransactionManager TransactionQueue.
func addTransactionToQueue(t Transaction) {
	transactionQueriesLock.Lock()
	tManager.TransactionQueue = append(tManager.TransactionQueue, t)
	transactionQueriesLock.Unlock()
}

//AddCommands prepares the commands to be run in a transaction.
func AddCommands(commands []common.Command) {
	tChannel <- newTransaction(commands)
}

/*StartTransactionManager is ment to be called once by the engine, afterwards it will receive incoming
transactions and will add them to its queue and then will excecute them by moving it into the excecution
batch.*/
func StartTransactionManager() {
	for {
		transaction := <-tChannel
		addTransactionToQueue(transaction)
		go transaction.excecuteTransaction()
	}
}
