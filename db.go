package mnemonic

import (
	"errors"
	"runtime/debug"
	"sync"
)

const (
	_         = iota
	tnxSet
	tnxGet
	tnxDelete
	tnxDrop
)

var (
	errorRecordNotFound  = errors.New("Record not found")
	errorCommandNotFound = errors.New("Command not found")
	errorNoDrop          = errors.New("Cannot drop table in transaction")
)

type Database struct {
	sync.RWMutex
	mIsInit         bool
	mData           map[string]interface{}
	mConnPool       *pool
	mChanWorkers    chan transaction
	mChanJob        chan transaction
	mThreadPoolSize int
	mConnPoolSize   int
}

type job struct {
	mType int
	mKey  string
	mData interface{}
}

type transaction struct {
	mJobs []job
	mRes  chan result
	mConn *Connection
}

type result struct {
	data interface{}
	err  error
}

type Config struct {
	ConnPoolSize   int
	ThreadPoolSize int
}

func (db *Database) transaction(tnx transaction) {
	res := &result{}
	for _, j := range tnx.mJobs {
		switch j.mType {
		case tnxSet:
			db.set(j)
		case tnxGet:
			res.data, res.err = db.get(j)
			break
		case tnxDelete:
			res.err = db.delete(j)
			if res.err != nil {
				break
			}
		case tnxDrop:
			db.drop(j)
		default:
			res.err = errorCommandNotFound
			break
		}
	}
	tnx.mConn.mChanResult <- *res
}

func (db *Database) set(j job) {
	db.mData[j.mKey] = j.mData
}

func (db *Database) get(j job) (interface{}, error) {
	if data, ok := db.mData[j.mKey]; ok {
		return data, nil
	}
	return nil, errorRecordNotFound
}

func (db *Database) delete(j job) error {
	if _, ok := db.mData[j.mKey]; !ok {
		return errorRecordNotFound
	}
	delete(db.mData, j.mKey)
	return nil
}

func (db *Database) drop(j job) {
	db.mData = make(map[string]interface{})
	debug.FreeOSMemory()
}

func (db *Database) do() {
	db.createWorkers()
	go func() {
		for {
			select {
			case tnx := <-db.mChanJob:
				db.transaction(tnx)
			}
			//TODO: Add break
		}
	}()
}

func (db *Database) createWorkers() {
	for i := 0; i < db.mThreadPoolSize; i++ {
		go func() {
			for {
				select {
				case tnx := <-db.mChanWorkers:
					db.mChanJob <- tnx
				}
				//TODO: Add break
			}
		}()
	}
}

func (db *Database) Conn() Connection {
	c := db.mConnPool.getConn()
	c.mDB = db
	c.mTnxRecords = make(map[string]interface{})
	c.mTnxDeleteRecords = make(map[string]interface{})
	c.mChanResult = make(chan result, 1)
	return c
}

func NewDB(config *Config) *Database {
	if config.ThreadPoolSize == 0 {
		config.ThreadPoolSize = 100
	}
	db := &Database{mData: make(map[string]interface{}),
		mIsInit: true,
		mConnPool: newConnPool(config.ConnPoolSize),
		mChanWorkers: make(chan transaction, config.ThreadPoolSize),
		mChanJob: make(chan transaction, 1),
		mThreadPoolSize: config.ThreadPoolSize,
		mConnPoolSize: config.ConnPoolSize}
	db.do()
	return db
}
