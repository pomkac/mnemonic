package mnemonic

type Connection struct {
	ID                int
	mDB               *Database
	mIsTnx            bool
	mTnx              transaction
	mTnxRecords       map[string]interface{}
	mTnxDeleteRecords map[string]interface{}
	mChanResult       chan result
	isClosed          bool
}

func (c *Connection) set(key string, data interface{}) error {
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{{mType: tnxSet, mKey: key, mData: data}}
	c.mTnx.mConn = c
	c.mDB.mChanWorkers <- c.mTnx
	res := <-c.mChanResult
	return res.err
}

func (c *Connection) get(key string) (interface{}, error) {
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{{mType: tnxGet, mKey: key}}
	c.mTnx.mConn = c
	c.mDB.mChanWorkers <- c.mTnx
	res := <-c.mChanResult
	return res.data, res.err
}

func (c *Connection) delete(key string) error {
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{{mType: tnxDelete, mKey: key}}
	c.mTnx.mConn = c
	c.mDB.mChanWorkers <- c.mTnx
	res := <-c.mChanResult
	return res.err
}

func (c *Connection) drop() {
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{{mType: tnxDrop}}
	c.mTnx.mConn = c
	c.mDB.mChanWorkers <- c.mTnx
	 <-c.mChanResult
}

func (c *Connection) tnxSet(key string, data interface{}) {
	c.mTnxRecords[key] = data
}

func (c *Connection) tnxGet(key string) (data interface{}, err error) {

	if _, ok := c.mTnxDeleteRecords[key]; ok {
		return nil, errorRecordNotFound
	}

	if data, ok := c.mTnxRecords[key]; ok {
		return data, nil
	}

	data, err = c.get(key)

	if err != nil {
		return
	}

	c.mTnxRecords[key] = data
	return
}

func (c *Connection) tnxDelete(key string) {

	if _, ok := c.mTnxRecords[key]; ok {
		delete(c.mTnxRecords, key)
	}

	c.mTnxDeleteRecords[key] = nil
}

func (c *Connection) clearTransaction() {
	for k, _ := range c.mTnxRecords {
		delete(c.mTnxRecords, k)
	}

	for k, _ := range c.mTnxDeleteRecords {
		delete(c.mTnxDeleteRecords, k)
	}

	c.mIsTnx = false
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{}
	c.mTnx.mConn = c
}

func (c *Connection) Set(key string, data interface{}) error {
	if c.mIsTnx {
		c.tnxSet(key, data)
		return nil
	}
	return c.set(key, data)
}

func (c *Connection) Get(key string) (interface{}, error) {
	if c.mIsTnx {
		return c.tnxGet(key)
	}
	return c.get(key)
}

func (c *Connection) Delete(key string) {
	if c.mIsTnx {
		c.tnxDelete(key)
		return
	}
	c.delete(key)
}

func (c *Connection) Drop() error {
	if c.mIsTnx {
		return errorNoDrop
	}
	c.drop()
	return nil
}

func (c *Connection) BeginTransaction() {
	c.clearTransaction()
	c.mIsTnx = true
}

func (c *Connection) Commit() error {
	c.mTnx = transaction{}
	for k, _ := range c.mTnxDeleteRecords {
		c.mTnx.mJobs = append(c.mTnx.mJobs, job{mType: tnxDelete, mKey: k})
	}
	for k, v := range c.mTnxRecords {
		data := v
		c.mTnx.mJobs = append(c.mTnx.mJobs, job{mType: tnxSet, mKey: k, mData: data})
	}
	c.mTnx.mConn = c

	c.mDB.mChanWorkers <- c.mTnx
	res := <-c.mChanResult

	c.clearTransaction()
	return res.err
}

func (c *Connection) Rollback() {
	c.clearTransaction()
}

func (c *Connection) Close() {
	c.isClosed = true
	c.mDB.mConnPool.putConn(c)
}
