// This package allows you to organize full-duplex connection handling using Go primitives. In other words
// one can write and read to/from connection simultaneously and coordinate everything using channel operations
// and select{} statement.
//
// It is worth mentioning this package is not performance oriented.
//
// A typical usage scenario might look something like this:
//
//	func handleConnection(conn net.Conn, incoming chan Request) {
//		end := make(chan error)
//		cr := packeter.NewConnectionReader(packeter.ConnectionReaderConfig[Response]{
//			Conn: conn,
//			Read: packeter.ReadLineDecode(decodeResponse),
//			End:  end,
//		})
//		cw := packeter.NewConnectionWriter(packeter.ConnectionWriterConfig[Request]{
//			Write: packeter.EncodeWriteWithTimeout(conn, writeTimeout, encodeRequest),
//			End:   end,
//		})
//		onError := func(err1 error) {
//			cr.Stop()
//			cw.Stop()
//			conn.Close()
//			err2 := <-end
//			// log err1 and err2
//			// communicate back connection stop or failure
//		}
//		go func() {
//			for {
//				select {
//				case req := <-incoming:
//					if err := cw.Send(req); err != nil {
//						onError(err)
//					}
//				case err := <-end:
//					onError(err)
//				case resp := <-cr.C:
//					dispatchResponse(resp)
//				}
//			}
//		}()
//	}
package packeter

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// When ConnectionWriter or ConnectionReader stops gracefully, this error is returned via End channel.
var ErrGracefulStop = errors.New("graceful stop")

type ConnectionWriterConfig[T any] struct {
	// Writing function. Possibly use EncodeWriteWithTimeout for this.
	Write func(v *T) error
	// On exit ConnectionWriter sends a single non-nil error to this channel. If termination is graceful,
	// the error value is ErrGracefulStop.
	End chan error
}

type ConnectionWriter[T any] struct {
	wch    chan<- T
	stopCh chan<- struct{}
	end    <-chan error
}

// Request ConnectionWriter termination. ConnectionWriter may possibly be blocked waiting for write
// to complete. To ensure quicker response, close the underlying connection as well after calling Stop().
//
// Can only be called once.
func (cw *ConnectionWriter[T]) Stop() {
	if cw.stopCh == nil {
		panic("Stop() can only be called once")
	}
	cw.stopCh <- struct{}{}
	cw.stopCh = nil
}

// Send a message via connection. If non-nil error is returned, the message was never processed by
// ConnectionWriter.
func (cw *ConnectionWriter[T]) Send(msg T) error {
	select {
	case cw.wch <- msg:
		return nil
	case err := <-cw.end:
		return err
	}
}

// Create a ConnectionWriter. Use Send to send messages to it.
//
// Typically to work with ConnectionWriter one must do the following:
//
//	cw := NewConnectionWriter(ConnectionWriterConfig{
//	  End: end,
//	  // ... other fields ...
//	})
//	select {
//	case msg := <-incoming:
//	   if err := cw.Send(msg); err != nil {
//	    handleError(err)
//	  }
//	case err := <-end:
//	   handleError(err)
//	}
//
//	// and to forcefully terminate the writer
//	cw.Stop()
//	conn.Close() // assuming Write function writes to this connection
func NewConnectionWriter[T any](cfg ConnectionWriterConfig[T]) ConnectionWriter[T] {
	ch := make(chan T)
	stopCh := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case cmd := <-ch:
				if err := cfg.Write(&cmd); err != nil {
					cfg.End <- fmt.Errorf("connection writer failed: %w", err)
					return
				}
			case <-stopCh:
				cfg.End <- ErrGracefulStop
				return
			}
		}
	}()
	return ConnectionWriter[T]{
		wch:    ch,
		stopCh: stopCh,
		end:    cfg.End,
	}
}

type ConnectionReaderConfig[T any] struct {
	// Connection to read from.
	Conn io.Reader
	// Reading function. Possibly use ReadLineDecode for this.
	Read func(br *bufio.Reader) (T, error)
	// On exit ConnectionReader sends a single non-nil error to this channel. If termination is graceful,
	// the error value is ErrGracefulStop.
	End chan error
}

type ConnectionReader[T any] struct {
	C      <-chan T
	stopCh chan<- struct{}
}

// Request ConnectionReader termination. Note that in most cases ConnectionReader is blocked reading
// data from the connection, hence in addition to calling Stop() one should close the underlying connection.
//
// Can only be called once.
func (cr *ConnectionReader[T]) Stop() {
	if cr.stopCh == nil {
		panic("Stop() can only be called once")
	}
	cr.stopCh <- struct{}{}
	cr.stopCh = nil
}

// Create a ConnectionReader. It starts reading data from the connection right away.
//
// Typically to work with ConnectionReader one must do the following:
//
//	cr := NewConnectionReader(ConnectionReaderConfig{
//	  Conn: conn,
//	  End: end,
//	  // ... other fields ...
//	})
//	select {
//	case err := <-end:
//	   handleError(err)
//	case msg := <-cr.C:
//	   handleMessage(msg)
//	}
//
//	// and to forcefully terminate the reader
//	cr.Stop()
//	conn.Close()
func NewConnectionReader[T any](cfg ConnectionReaderConfig[T]) ConnectionReader[T] {
	ch := make(chan T)
	bconn := bufio.NewReader(cfg.Conn)
	stopCh := make(chan struct{}, 1)
	go func() {
		for {
			msg, err := cfg.Read(bconn)
			if err != nil {
				if errors.Is(err, ErrGracefulStop) {
					cfg.End <- ErrGracefulStop
				} else {
					cfg.End <- fmt.Errorf("connection reader failed: %w", err)
				}
				return
			}

			select {
			case ch <- msg:
			case <-stopCh:
				cfg.End <- ErrGracefulStop
				return
			}
		}
	}()
	return ConnectionReader[T]{
		C:      ch,
		stopCh: stopCh,
	}
}

// Creates a helper Read function for ConnectionReaderConfig. Function reads a line and then decodes
// it into a message using a provided function.
func ReadLineDecode[T any](decode func(data []byte) (T, error)) func(br *bufio.Reader) (T, error) {
	return func(br *bufio.Reader) (T, error) {
		data, err := br.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, net.ErrClosed) { // don't report ErrClosed as an error
				var x T
				return x, ErrGracefulStop
			} else {
				var x T
				return x, err
			}
		}

		return decode(data)
	}
}

type writeDeadliner interface {
	SetWriteDeadline(t time.Time) error
}

// Creates a helper Write function for ConnectionWriterConfig. Function encodes a messages using provided
// function and then writes it to a writer. If writer implements "SetWriteDeadline(t time.Time) error",
// deadline is set based on provided timeout value.
func EncodeWriteWithTimeout[T any](w io.Writer, timeout time.Duration, encode func(v *T) ([]byte, error)) func(v *T) error {
	wd, _ := w.(writeDeadliner)
	return func(v *T) error {
		data, err := encode(v)
		if err != nil {
			return err
		}

		if wd != nil {
			if err := wd.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
				return err
			}
		}

		if _, err := w.Write(data); err != nil {
			return err
		}

		return nil
	}
}
