package main

import (
	"io"
	"sync"
)

type AsyncWriter struct {
	qIn   chan []byte
	flush chan struct{}
}

func (w *AsyncWriter) Write(buf []byte) {
	w.qIn <- buf
}

func (w *AsyncWriter) Flush() {
	w.flush <- struct{}{}
}

func (w *AsyncWriter) Close() {
	close(w.qIn)
}

func NewAsyncWriter(out io.Writer, bufSize int) *AsyncWriter {
	qIn := make(chan []byte)
	qOut := make(chan []byte)
	flush := make(chan struct{})
	bufPool := sync.Pool{New: func() interface{} { return make([]byte, 0, bufSize) }}

	go func() {
		buf := bufPool.Get().([]byte)

	poll:
		for {
			select {
			case msg, ok := <-qIn:
				if !ok {
					qOut <- buf
					close(qOut)
					return
				}
				if len(buf) == 0 && len(msg) > 0 {
					select {
					case qOut <- msg:
						continue poll // lucky
					default:
					}
				}
				buf = append(buf, msg...)
				select {
				case qOut <- buf:
					buf = bufPool.Get().([]byte)
				default:
				}
			case <-flush:
				if len(buf) > 0 {
					qOut <- buf
					buf = bufPool.Get().([]byte)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case buf, ok := <-qOut:
				if !ok {
					return
				}
				out.Write(buf)
				bufPool.Put(buf[0:0])
				go func() { flush <- struct{}{} }()
			}
		}
	}()

	return &AsyncWriter{qIn: qIn, flush: flush}
}
