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

type database struct {
	sync.RWMutex
	mIsInit   bool
	mData     map[string]interface{}
	mConnPool *pool
}

type job struct {
	mType int
	mKey  string
	mData interface{}
}

type transaction struct {
	mJobs []job
	mRes  chan result
}

type result struct {
	data interface{}
	err  error
}

func (db *database) transaction(tnx *transaction) result{
	res := &result{}
	for _, j := range tnx.mJobs {
		switch j.mType {
		case tnxSet:
			db.set(&j)
		case tnxGet:
			res.data, res.err = db.get(&j)
			break
		case tnxDelete:
			res.err = db.delete(&j)
			if res.err != nil {
				break
			}
		case tnxDrop:
			db.drop(&j)
		default:
			res.err = errorCommandNotFound
			break
		}
	}
	return *res
}

func (db *database) set(j *job) {
	db.Lock()
	db.mData[j.mKey] = j.mData
	db.Unlock()
}

func (db *database) get(j *job) (interface{}, error) {
	db.RLock()
	defer db.RUnlock()
	if data, ok := db.mData[j.mKey]; ok {
		return data, nil
	}
	return nil, errorRecordNotFound
}

func (db *database) delete(j *job) error {
	db.Lock()
	defer db.Unlock()
	if _, ok := db.mData[j.mKey]; !ok {
		return errorRecordNotFound
	}
	delete(db.mData, j.mKey)
	return nil
}

func (db *database) drop(j *job) {
	db.Lock()
	db.mData = make(map[string]interface{})
	debug.FreeOSMemory()
	db.Unlock()
}

func (db *database) Conn() Connection {
	c := db.mConnPool.getConn()
	c.mDB = db
	c.mTnxRecords = make(map[string]interface{})
	c.mTnxDeleteRecords = make(map[string]interface{})
	c.mChanClear = make(chan bool)
	return c
}

func NewDB() *database {
	db := &database{mData: make(map[string]interface{}),
		mIsInit: true,
		mConnPool: newPool()}
	return db
}
