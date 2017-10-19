package mnemonic

type pool struct {
	connPool chan Connection
}

func newConnPool(size int) *pool {
	if size == 0{
		size = 100
	}
	return &pool{
		make(chan Connection, size),
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
	c = &Connection{}
	select {
	case p.connPool <- *c:
	default:
	}
	return
}
