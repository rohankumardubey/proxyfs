package rpctest

// Simple ping for testing the RPC layer
import (
	"bytes"
	"fmt"

	"github.com/swiftstack/ProxyFS/blunder"
)

func encodeErrno(e *error) {
	if *e != nil {
		*e = fmt.Errorf("errno: %d", blunder.Errno(*e))
	}
}

// RpcPing simply does a len on the message path and returns the result
func (s *Server) RpcPing(in *PingReq, reply *PingReply) (err error) {

	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return nil
}

var largeStr string = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"

// RpcPingLarge simply does a len on the message path and returns the result
// along with a larger buffer.
func (s *Server) RpcPingLarge(in *PingReq, reply *PingReply) (err error) {

	buf := bytes.Buffer{}
	p := fmt.Sprintf("pong %d bytes", len(in.Message))
	buf.WriteString(p)
	for i := 0; i < 1000; i++ {
		buf.WriteString(largeStr)
	}

	reply.Message = fmt.Sprintf("%v", buf.String())
	return nil
}

// RpcPingWithError returns an error
func (s *Server) RpcPingWithError(in *PingReq, reply *PingReply) (err error) {
	err = blunder.AddError(err, blunder.NotFoundError)
	encodeErrno(&err)
	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return err
}
