package logbuf

import (
	"io"
	"sync"
	"time"

	"github.com/cafxx/gcnotifier"
)

type BufferingWriter struct {
	w         io.Writer
	flushSize int
	flushTime time.Duration
	buf       []byte
	l         sync.Mutex
	closeCh   chan struct{}
	flushReq  bool
	err       error
}

func NewManualFlushBufferingWriter(w io.Writer) *BufferingWriter {
	return NewBufferingWriter(w, 0, 0)
}

func NewSizeBufferingWriter(w io.Writer, flushSize int) *BufferingWriter {
	return NewBufferingWriter(w, 0, flushSize)
}

func NewTimeBufferingWriter(w io.Writer, flushTime time.Duration) *BufferingWriter {
	return NewBufferingWriter(w, flushTime, 0)
}

func NewDefaultBufferingWriter(w io.Writer) *BufferingWriter {
	return NewBufferingWriter(w, 100*time.Millisecond, 4096)
}

func NewBufferingWriter(w io.Writer, flushTime time.Duration, flushSize int) *BufferingWriter {
	bw := &BufferingWriter{
		w:         w,
		flushTime: flushTime,
		flushSize: flushSize,
		closeCh:   make(chan struct{}),
	}
	go bw.run()
	return bw
}

func (w *BufferingWriter) Write(buf []byte) (int, error) {
	w.l.Lock()
	defer w.l.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	if len(w.buf)+len(buf) >= w.flushSize {
		w.flush(true)
		if w.err != nil {
			return 0, w.err
		}
		if len(buf) >= w.flushSize {
			if err := w.writeall(buf); err != nil {
				return 0, err
			}
			return len(buf), nil
		}
	}
	w.buf = append(w.buf, buf...)
	if w.flushReq == false && len(w.buf) > w.flushSize/2 {
		w.flushReq = true
		go w.Flush(true)
	}
	return len(buf), nil
}

func (w *BufferingWriter) Flush(reuseBuf bool) {
	w.l.Lock()
	w.flush(reuseBuf)
	w.l.Unlock()
}

func (w *BufferingWriter) writeall(buf []byte) error {
	for len(buf) > 0 {
		written, err := w.w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[written:]
	}
	return nil
}

func (w *BufferingWriter) flush(reuseBuf bool) {
	if w.err != nil {
		return
	}
	if err := w.writeall(w.buf); err != nil {
		w.err = err
		return
	}
	if reuseBuf {
		w.buf = w.buf[:0]
	} else {
		w.buf = nil
	}
	w.flushReq = false
}

func (w *BufferingWriter) Close() {
	w.closeCh <- struct{}{}
	w.Flush(false)
}

func (w *BufferingWriter) run() {
	var flushTimer <-chan time.Time
	if w.flushTime > 0 {
		t := time.NewTicker(w.flushTime)
		defer t.Stop()
		flushTimer = t.C
	}

	gcn := gcnotifier.New()
	defer gcn.Close()
	gc := gcn.AfterGC()

	for {
		select {
		case <-gc:
			w.Flush(false)
		case <-flushTimer:
			w.Flush(true)
		case <-w.closeCh:
			return
		}
	}
}
