package main

import (
	"errors"
	"net"
	"sync"
	"time"
)

//ErrClosed occurs when Len() or Get() is called after Close() is
var (
	ErrClosed  = errors.New("pool is closed")
	ErrTimeout = errors.New("timeout")
)

//Pool interface describes a connection pool.
type pool interface {
	//Get returns an available(new or reused) connection from the pool.
	//It blocks when no connection is available.
	//Closing a connection puts the connection back to the pool.
	//Duplicated close of a connection is endurable as the second close is ignored.
	Get() (net.Conn, error)

	//Close closes the pool and all its connections.
	//Call Get() when the pool is closed is counted as an error.
	Close()

	//Len returns the current number of connections in the pool,
	//including those that are in use or free.
	Len() int
}

//blockingPool implements the pool interface.
//Connestions from blockingPool offer a kind of blocking mechanism that is derived from buffered channel.
type blockingPool struct {
	//mutex is to make closing the pool and recycling the connection an atomic operation
	mutex sync.Mutex

	//timeout to Get, default to 3
	timeout time.Duration

	//storage for net.Conn connections
	conns chan *wrappedConn

	//net.Conn generator
	factory factory

	livetime time.Duration
}

//factory is a function to create new connections
//which is provided by the user
type factory func() (net.Conn, error)

//Create a new blocking pool. As no new connections would be made when the pool is busy,
//the number of connections of the pool is kept no more than initCap and maxCap does not
//make sense but the api is reserved. The timeout to block Get() is set to 3 by default
//concerning that it is better to be related with Get() method.
func newblockingPool(initCap, maxCap int, livetime time.Duration, ft factory) (pool, error) {
	if initCap < 0 || maxCap < 1 || initCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	newPool := &blockingPool{
		timeout:  3,
		conns:    make(chan *wrappedConn, maxCap),
		factory:  ft,
		livetime: livetime,
	}

	for i := 0; i < initCap; i++ {
		newPool.conns <- newPool.wrap(nil)
	}
	return newPool, nil
}

//Get blocks for an available connection.
func (p *blockingPool) Get() (net.Conn, error) {
	//in case that pool is closed or pool.conns is set to nil
	conns := p.conns
	if conns == nil {
		return nil, ErrClosed
	}

	select {
	case conn := <-conns:
		if time.Since(conn.start) > p.livetime {
			if conn.Conn != nil {
				conn.Conn.Close()
				conn.Conn = nil
			}
		}
		if conn.Conn == nil {
			var err error
			conn.Conn, err = p.factory()
			if err != nil {
				conn.start = time.Now()
				p.put(conn)
				return nil, err
			}
		}
		conn.unusable = false
		return conn, nil
	case <-time.After(time.Second * p.timeout):
		return nil, ErrTimeout
	}
}

//put puts the connection back to the pool. If the pool is closed, put simply close
//any connections received and return immediately. A nil net.Conn is illegal and will be rejected.
func (p *blockingPool) put(conn *wrappedConn) error {
	//in case that pool is closed and pool.conns is set to nil
	conns := p.conns
	if conns == nil {
		//conn.Conn is possibly nil coz factory() may fail, in which case conn is immediately
		//put back to the pool
		if conn.Conn != nil {
			conn.Conn.Close()
			conn.Conn = nil
		}
		return ErrClosed
	}

	//if conn is marked unusable, underlying net.Conn is set to nil
	if conn.unusable {
		if conn.Conn != nil {
			conn.Conn.Close()
			conn.Conn = nil
		}
	}

	//It is impossible to block as number of connections is never more than length of channel
	conns <- conn
	return nil
}

//TODO
//Close set connection channel to nil and close all the relative connections.
//Yet not implemented.
func (p *blockingPool) Close() {}

//TODO
//Len return the number of current active(in use or available) connections.
func (p *blockingPool) Len() int {
	return 0
}

func (p *blockingPool) wrap(conn net.Conn) *wrappedConn {
	return &wrappedConn{
		conn,
		p,
		false,
		time.Now(),
	}
}

//WrappedConn modify the behavior of net.Conn's Write() method and Close() method
//while other methods can be accessed transparently.
type wrappedConn struct {
	net.Conn
	pool     *blockingPool
	unusable bool
	start    time.Time
}

//TODO
func (c *wrappedConn) Close() error {
	return c.pool.put(c)
}

//Write checkout the error returned from the origin Write() method.
//If the error is not nil, the connection is marked as unusable.
func (c *wrappedConn) Write(b []byte) (n int, err error) {
	//c.Conn is certainly not nil
	n, err = c.Conn.Write(b)
	if err != nil {
		c.unusable = true
	} else {
		c.start = time.Now()
	}
	return
}

//Read works the same as Write.
func (c *wrappedConn) Read(b []byte) (n int, err error) {
	//c.Conn is certainly not nil
	n, err = c.Conn.Read(b)
	if err != nil {
		c.unusable = true
	} else {
		c.start = time.Now()
	}
	return
}
