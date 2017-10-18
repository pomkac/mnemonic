package mnemonic

type connection struct {
	ID                int
	mDB               *database
	mIsTnx            bool
	mTnx              transaction
	mTnxRecords       map[string]interface{}
	mTnxDeleteRecords map[string]interface{}
	mChanClear        chan bool
	isClosed		  bool
}

func (c *connection) set(key string, data interface{}) error {
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{{mType: tnxSet, mKey: key, mData: data}}
	c.mTnx.mRes = make(chan result)
	c.mDB.mTnx <- c.mTnx
	res := <-c.mTnx.mRes
	return res.err
}

func (c *connection) get(key string) (interface{}, error) {
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{{mType: tnxGet, mKey: key}}
	c.mTnx.mRes = make(chan result)
	c.mDB.mTnx <- c.mTnx
	res := <-c.mTnx.mRes
	return res.data, res.err
}

func (c *connection) delete(key string) error {
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{{mType: tnxDelete, mKey: key}}
	c.mTnx.mRes = make(chan result)
	c.mDB.mTnx <- c.mTnx
	res := <-c.mTnx.mRes
	return res.err
}

func (c *connection) drop() {
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{{mType: tnxDrop}}
	c.mTnx.mRes = make(chan result)
	c.mDB.mTnx <- c.mTnx
	_ = <-c.mTnx.mRes
}

func (c *connection) tnxSet(key string, data interface{}) {
	c.mTnxRecords[key] = data
}

func (c *connection) tnxGet(key string) (data interface{}, err error) {

	if _, ok := c.mTnxDeleteRecords[key]; ok {
		return nil, errorRecordNotFound
	}

	if data, ok := c.mTnxRecords[key]; ok{
		return data, nil
	}

	data, err = c.get(key)

	if err != nil {
		return
	}

	c.mTnxRecords[key] = data
	return
}

func (c *connection) tnxDelete(key string) {

	if _, ok := c.mTnxRecords[key]; ok{
		delete(c.mTnxRecords, key)
	}

	c.mTnxDeleteRecords[key] = nil
}

func (c *connection) clearTransaction() {
	go func(cl chan bool) {
		for k, _ := range c.mTnxRecords {
			delete(c.mTnxRecords, k)
		}
		cl <- true
	}(c.mChanClear)

	go func(cl chan bool) {
		for k, _ := range c.mTnxDeleteRecords {
			delete(c.mTnxDeleteRecords, k)
		}
		cl <- true
	}(c.mChanClear)

	c.mIsTnx = false
	c.mTnx = transaction{}
	c.mTnx.mJobs = []job{}
	c.mTnx.mRes = make(chan result)

	_,_ = <-c.mChanClear, <-c.mChanClear
}

func (c *connection) Set(key string, data interface{}) error {
	if c.mIsTnx {
		c.tnxSet(key, data)
		return nil
	}
	return c.set(key, data)
}

func (c *connection) Get(key string) (interface{}, error) {
	if c.mIsTnx {
		return c.tnxGet(key)
	}
	return c.get(key)
}

func (c *connection) Delete(key string){
	if c.mIsTnx {
		c.tnxDelete(key)
		return
	}
	c.delete(key)
}

func (c *connection) Drop() error{
	if c.mIsTnx{
		return errorNoDrop
	}
	c.drop()
	return nil
}

func (c *connection) BeginTransaction() {
	c.clearTransaction()
	c.mIsTnx = true
}

func (c *connection) Commit() error {
	c.mTnx = transaction{}
	for k, _ := range c.mTnxDeleteRecords{
		c.mTnx.mJobs = append(c.mTnx.mJobs, job{mType: tnxDelete, mKey: k})
	}
	for k, v := range c.mTnxRecords{
		data := v
		c.mTnx.mJobs = append(c.mTnx.mJobs, job{mType: tnxSet, mKey: k, mData: data})
	}
	c.mTnx.mRes = make(chan result)
	c.mDB.mTnx <- c.mTnx
	res := <-c.mTnx.mRes
	c.clearTransaction()
	return res.err
}

func (c *connection) Rollback() {
	c.clearTransaction()
}

func (c *connection) Close() {
	c.isClosed = true
	c.mDB.mConnPool.putConn(c)
}
