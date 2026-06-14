package wal

// startWriter launches the single writer goroutine (the Singular Update Queue).
// In this task it only handles shutdown; append handling is added in Task 10.
func (w *WAL) startWriter() {
	w.wg.Add(1)
	go w.writerLoop()
}

// writerLoop is the body of the single writer goroutine. It runs until the WAL
// is closed, then performs a final flush of the active segment.
func (w *WAL) writerLoop() {
	defer w.wg.Done()

	<-w.closed
	w.finalFlush()
}

// finalFlush fsyncs the active segment during shutdown, logging any failure.
func (w *WAL) finalFlush() {
	if w.active == nil {
		return
	}

	if err := w.active.Sync(); err != nil {
		w.opts.logger.Error("wal: final flush failed", Field{Key: "error", Value: err.Error()})
	}
}

// startFlusher launches the periodic background fsync goroutine for
// SyncInterval. It is expanded in Task 11.
func (w *WAL) startFlusher() {}
