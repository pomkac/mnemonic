package mnemonic

type pool struct {
	connPool chan connection
}

func newPool() *pool {
	return &pool{
		make(chan connection, 500),
	}
}

func (p *pool) getConn() (c connection) {
	select {
	case conn, ok := <-p.connPool:
		if ok {
			return conn
		}
	default:
	}
	return
}

func (p *pool) putConn(c *connection) {
	*c = connection{}
	select {
	case p.connPool <- *c:
	default:
	}
	return
}
