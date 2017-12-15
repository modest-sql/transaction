package transaction

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/modest-sql/common"
	"github.com/rs/xid"
)

//execWaitGroup is used to make suretransactions run in order.
var execWaitGroup sync.WaitGroup

//Transaction States.
const (
	Queued = iota
	InProgress
	Done
)

//transactionLock is the global transaction rwmutex.
var transactionLock sync.RWMutex

//transactionQueriesLock is the global transaction queries lock.
var transactionQueriesLock sync.Mutex

//excecutionBatchLock is the excecution batch lock.
var excecutionBatchLock sync.Mutex

//tChannel is used to communicate with StartTransactionManager
var tChannel = make(chan Transaction)

//tManager is the Transaction Manager global instance.
var transactionManager manager

var config TransactionConfiguration

//TransactionConfiguration contains configuration options read from a json file.
type TransactionConfiguration struct {
	//commandsDelay determines the delay in seconds between the excecution of each command in a transaction.
	CommandsDelay json.Number `json:"excecution_delay"`

	//transactionThreads determines the ammount of transactions to be run concurrently.
	TransactionThreads json.Number `json:"execution_batch_size"`
}

func init() {
	jsonFile, _ := os.Open("config.json")

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	json.Unmarshal([]byte(byteValue), &config)
}

//Transaction instance definition.
type Transaction struct {
	TransactionID         xid.ID   `json:"Transaction_ID"`
	TransactionQueries    []string `json:"TransactionQueries"`
	CommandsInTransaction []common.Command
	TransactionState      int    `json:"Transaction_State"`
	CurrentComand         string `json:"Current_Command"`
}

//NewTransaction creates an instance of Transaction.
func NewTransaction(commands []common.Command) Transaction {
	T := Transaction{
		TransactionID:         xid.New(),
		commandsInTransaction: commands,
		TransactionState:      Queued,
		CurrentComand:         "None in excecution yet.",
	}

	for _, command := range commands {
		T.TransactionQueries = append(T.TransactionQueries, command.String())
	}

	return T
}

//excecuteTransaction excectes the commands inside a transaction.
func (T *Transaction) excecuteTransaction() {
	T.TransactionState = InProgress

	MutexesMap := new(sync.Map)

	//ADD A RWMUTEX FOR EACH TABLE INVOLVED IN THIS TRANSACTION EXCECUTION QUEUE.
	for i := 0; i < len(T.CommandsInTransaction); i++ {
		MutexesMap.Store(T.CommandsInTransaction[i].TableName(), &sync.RWMutex{})
	}

	//INSERT LOCKS IN-BETWEEN COMMANDS.
	XSLOCKEDTRANSACTION := make([]interface{}, 0)

	for i := 0; i < len(T.CommandsInTransaction); i++ {
		if T.CommandsInTransaction[i].InstructionType == 1 {
			XSLOCKEDTRANSACTION = append(XSLOCKEDTRANSACTION, "RLOCK")
			XSLOCKEDTRANSACTION = append(XSLOCKEDTRANSACTION, T.CommandsInTransaction[i])
			XSLOCKEDTRANSACTION = append(XSLOCKEDTRANSACTION, "RUNLOCK")
		} else {
			XSLOCKEDTRANSACTION = append(XSLOCKEDTRANSACTION, "LOCK")
			XSLOCKEDTRANSACTION = append(XSLOCKEDTRANSACTION, T.CommandsInTransaction[i])
			XSLOCKEDTRANSACTION = append(XSLOCKEDTRANSACTION, "UNLOCK")
		}
	}

	//EXCECUTE TRANSACTION EXCECUTION QUEUE.
	for i := 0; i < len(XSLOCKEDTRANSACTION); i++ {
		switch C := XSLOCKEDTRANSACTION[i].(type) {
		case string:
			if C == "RLOCK" {
				Lock, _ := MutexesMap.Load((XSLOCKEDTRANSACTION[i+1].(common.Command)).TableName())
				(Lock.(*sync.RWMutex)).RLock()
			} else if C == "RUNLOCK" {
				Lock, _ := MutexesMap.Load((XSLOCKEDTRANSACTION[i-1].(common.Command)).TableName())
				(Lock.(*sync.RWMutex)).RUnlock()
			} else if C == "LOCK" {
				Lock, _ := MutexesMap.Load((XSLOCKEDTRANSACTION[i+1].(common.Command)).TableName())
				(Lock.(*sync.RWMutex)).Lock()
			} else if C == "UNLOCK" {
				Lock, _ := MutexesMap.Load((XSLOCKEDTRANSACTION[i-1].(common.Command)).TableName())
				(Lock.(*sync.RWMutex)).Unlock()
			}
		case common.Command:
			T.CurrentComand = C.String()
			C.Instruction()
			Delay, _ := config.CommandsDelay.Int64()
			time.Sleep(time.Second * time.Duration(Delay))
			T.CurrentComand = "Waiting..."
		default:
			fmt.Println("Unknown")
		}

	T.TransactionState = Done
	T.CurrentComand = "Transaction Finished."
	execWaitGroup.Done()

}

//manager instance definition.
type manager struct {
	TransactionQueue []Transaction `json:"TransactionQueue"`
}

//GetTransactions returns the array of transactions in memory.
func GetTransactions() []Transaction {
	Transactions := make([]Transaction, 0)

	transactionQueriesLock.Lock()
	for _, transaction := range transactionManager.TransactionQueue {
		Transactions = append(Transactions, transaction)
	}
	transactionQueriesLock.Unlock()

	return Transactions
}

//popTransactionQueue pops the first transaction in the queue.
func popTransactionQueue() {
	transactionQueriesLock.Lock()
	transactionManager.TransactionQueue = transactionManager.TransactionQueue[1:]
	transactionQueriesLock.Unlock()
}

//addTransactionToQueue adds a new Transaction to the TransactionManager TransactionQueue.
func addTransactionToQueue(t Transaction) {
	transactionQueriesLock.Lock()
	transactionManager.TransactionQueue = append(transactionManager.TransactionQueue, t)
	transactionQueriesLock.Unlock()
}

//AddCommands prepares the commands to be run in a transaction.
func AddCommands(commands []common.Command) {
	tChannel <- NewTransaction(commands)
}

func receiveTransactions() {
	for {
		transaction := <-tChannel
		addTransactionToQueue(transaction)
	}
}

/*StartTransactionManager is ment to be called once by the engine, afterwards it will receive incoming
transactions and will add them to its queue and then will excecute them by moving it into the excecution
batch.*/
func StartTransactionManager() {
	go receiveTransactions()
	for {
		excecutionBatchLock.Lock()
		executeBatch(len(transactionManager.TransactionQueue))
	}
}

func executeBatch(actualBatchSize int) {
	Threads, _ := config.TransactionThreads.Int64()
	if len(transactionManager.TransactionQueue) > 0 {
		if actualBatchSize < int(Threads) {
			//DETERMINE TRANSACTION'S RELEVANCE.
			TRANSACTIONDESIGNATION := make([]int, actualBatchSize)
			for i := 0; i < actualBatchSize; i++ {
				for j := 0; j < len(transactionManager.TransactionQueue[i].CommandsInTransaction); j++ {
					if transactionManager.TransactionQueue[i].CommandsInTransaction[j].InstructionType == 0 {
						if TRANSACTIONDESIGNATION[i] != 2 {
							TRANSACTIONDESIGNATION[i] = 1
						}
					} else if transactionManager.TransactionQueue[i].CommandsInTransaction[j].InstructionType == 4 || transactionManager.TransactionQueue[i].CommandsInTransaction[j].InstructionType == 5 {
						TRANSACTIONDESIGNATION[i] = 2
					} else {
						//Do nothing
					}
				}
			}

			//REORDER TRANSACTIONS IN A NEW ARRAY.
			REORDEREDTRANSACTIONS := make([]Transaction, 0)

			for i := 0; i < actualBatchSize; i++ {
				if TRANSACTIONDESIGNATION[i] == 1 {
					REORDEREDTRANSACTIONS = append(REORDEREDTRANSACTIONS, transactionManager.TransactionQueue[i])
				}
			}
			for i := 0; i < actualBatchSize; i++ {
				if TRANSACTIONDESIGNATION[i] == 0 {
					REORDEREDTRANSACTIONS = append(REORDEREDTRANSACTIONS, transactionManager.TransactionQueue[i])
				}
			}

			for i := 0; i < actualBatchSize; i++ {
				if TRANSACTIONDESIGNATION[i] == 2 {
					REORDEREDTRANSACTIONS = append(REORDEREDTRANSACTIONS, transactionManager.TransactionQueue[i])
				}
			}

			//REPLACE REORDERED TRANSACTIONS IN TRANSACTION QUEUE.
			for i := 0; i < actualBatchSize; i++ {
				transactionManager.TransactionQueue[i] = REORDEREDTRANSACTIONS[i]
			}

			//MAKE SURE TRANSACTION RUNS COMMANDS IN ORDER.
			execWaitGroup.Add(Contains(TRANSACTIONDESIGNATION, 1))
			for i := 0; i < Contains(TRANSACTIONDESIGNATION, 1); i++ {
				go transactionManager.TransactionQueue[i].excecuteTransaction()
			}
			execWaitGroup.Wait()
			execWaitGroup.Add(Contains(TRANSACTIONDESIGNATION, 0))
			for i := 0; i < Contains(TRANSACTIONDESIGNATION, 0); i++ {
				go transactionManager.TransactionQueue[i].excecuteTransaction()
			}
			execWaitGroup.Wait()
			execWaitGroup.Add(Contains(TRANSACTIONDESIGNATION, 2))
			for i := 0; i < Contains(TRANSACTIONDESIGNATION, 2); i++ {
				go transactionManager.TransactionQueue[i].excecuteTransaction()
			}
			execWaitGroup.Wait()

			//POP DONE TRANSACTIONS FROM QUEUE.
			for i := 0; i < actualBatchSize; i++ {
				popTransactionQueue()
			}

		} else {
			//DETERMINE TRANSACTION'S RELEVANCE.
			TRANSACTIONDESIGNATION := make([]int, Threads)
			for i := 0; i < int(Threads); i++ {
				for j := 0; j < len(transactionManager.TransactionQueue[i].CommandsInTransaction); j++ {
					if transactionManager.TransactionQueue[i].CommandsInTransaction[j].InstructionType == 0 {
						if TRANSACTIONDESIGNATION[i] != 2 {
							TRANSACTIONDESIGNATION[i] = 1
						}
					} else if transactionManager.TransactionQueue[i].CommandsInTransaction[j].InstructionType == 4 || transactionManager.TransactionQueue[i].CommandsInTransaction[j].InstructionType == 5 {
						TRANSACTIONDESIGNATION[i] = 2
					} else {
						//Do nothing
					}
				}
			}

			//REORDER TRANSACTIONS IN A NEW ARRAY.
			REORDEREDTRANSACTIONS := make([]Transaction, 0)

			for i := 0; i < int(Threads); i++ {
				if TRANSACTIONDESIGNATION[i] == 1 {
					REORDEREDTRANSACTIONS = append(REORDEREDTRANSACTIONS, transactionManager.TransactionQueue[i])
				}
			}
			for i := 0; i < int(Threads); i++ {
				if TRANSACTIONDESIGNATION[i] == 0 {
					REORDEREDTRANSACTIONS = append(REORDEREDTRANSACTIONS, transactionManager.TransactionQueue[i])
				}
			}

			for i := 0; i < int(Threads); i++ {
				if TRANSACTIONDESIGNATION[i] == 2 {
					REORDEREDTRANSACTIONS = append(REORDEREDTRANSACTIONS, transactionManager.TransactionQueue[i])
				}
			}

			//REPLACE REORDERED TRANSACTIONS IN TRANSACTION QUEUE.
			for i := 0; i < int(Threads); i++ {
				transactionManager.TransactionQueue[i] = REORDEREDTRANSACTIONS[i]
			}

			//MAKE SURE TRANSACTION RUNS COMMANDS IN ORDER.
			execWaitGroup.Add(Contains(TRANSACTIONDESIGNATION, 1))
			for i := 0; i < Contains(TRANSACTIONDESIGNATION, 1); i++ {
				go transactionManager.TransactionQueue[i].excecuteTransaction()
			}
			execWaitGroup.Wait()
			execWaitGroup.Add(Contains(TRANSACTIONDESIGNATION, 0))
			for i := 0; i < Contains(TRANSACTIONDESIGNATION, 0); i++ {
				go transactionManager.TransactionQueue[i].excecuteTransaction()
			}
			execWaitGroup.Wait()
			execWaitGroup.Add(Contains(TRANSACTIONDESIGNATION, 2))
			for i := 0; i < Contains(TRANSACTIONDESIGNATION, 2); i++ {
				go transactionManager.TransactionQueue[i].excecuteTransaction()
			}
			execWaitGroup.Wait()

			//POP DONE TRANSACTIONS FROM QUEUE.
			for i := 0; i < int(Threads); i++ {
				popTransactionQueue()
			}
		}
	}
	excecutionBatchLock.Unlock()
}

//Contains determines if an int array contains a certain value, returns the amount of time the value is found.
func Contains(s []int, e int) int {
	c := 0
	for _, a := range s {
		if a == e {
			c++
		}
	}
	return c
}
