package protocol

import (
	"net/rpc"
)

type Connection struct {
	Network string
	Address string
}

func Invoke(conn Connection, method string, args, reply any) error {
	c, err := rpc.Dial(conn.Network, conn.Address)
	if err != nil {
		return err
	}

	err = c.Call(method, args, reply)
	if err != nil {
		return err
	}

	return nil
}
