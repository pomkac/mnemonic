package mnemonic

type pool struct {
	connPool chan Connection
}

func newPool() *pool {
	return &pool{
		make(chan Connection, 500),
	}
}

func (p *pool) getConn() (c Connection) {
	select {
	case conn, ok := <-p.connPool:
		if ok {
			return conn
		}
	default:
	}
	return
}

func (p *pool) putConn(c *Connection) {
	*c = Connection{}
	select {
	case p.connPool <- *c:
	default:
	}
	return
}
