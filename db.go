package mnemonic

import (
	"errors"
	"runtime/debug"
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
	mIsInit   bool
	mData     map[string]interface{}
	mTnx      chan transaction
	mClose    chan bool
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

func (db *database) transaction(tnx *transaction) {
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
	tnx.mRes <- *res
}

func (db *database) set(j *job) {
	db.mData[j.mKey] = j.mData
}

func (db *database) get(j *job) (interface{}, error) {
	if data, ok := db.mData[j.mKey]; ok {
		return data, nil
	}
	return nil, errorRecordNotFound
}

func (db *database) delete(j *job) error {
	if _, ok := db.mData[j.mKey]; !ok {
		return errorRecordNotFound
	}
	delete(db.mData, j.mKey)
	return nil
}

func (db *database) drop(j *job) {
	db.mData = make(map[string]interface{})
	debug.FreeOSMemory()
}

func (db *database) do() {
	go func() {
		for {
			select {
			case tnx := <-db.mTnx:
				db.transaction(&tnx)
			case <-db.mClose:
				return
			default:
			}
		}
	}()
}

func (db *database) Conn() Connection {
	c := db.mConnPool.getConn()
	c.mDB = db
	c.mTnxRecords = make(map[string]interface{})
	c.mTnxDeleteRecords = make(map[string]interface{})
	c.mChanClear = make(chan bool)
	return c
}

func (db *database) Close() {
	db.mClose <- true
}

func NewDB() *database {
	db := &database{mData: make(map[string]interface{}),
		mTnx: make(chan transaction),
		mClose: make(chan bool),
		mIsInit: true,
		mConnPool: newPool()}
	db.do()
	return db
}
