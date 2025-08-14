package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"k8s.io/klog/v2"
)

type RangeType int

const (
	RangeTypeKey RangeType = iota
	RangeTypeRangeNoEnd
	RangeTypeRangeNormal
)

// Watch creates a watcher for the given key/range starting from the specified revision
// If rangeEnd is empty, it watches a single key.
// If rangeEnd is specified, it watches the range [key, rangeEnd).
func (m *MemoryStorage) Watch(ctx context.Context, req *etcdserverpb.WatchCreateRequest, callback func(event *etcdserverpb.WatchResponse) error) (Watcher, Revision, error) {
	m.watcherMu.Lock()
	defer m.watcherMu.Unlock()

	// TODO: Assign req.StartRevision to the watch?

	watcherID := req.GetWatchId()
	if watcherID == 0 {
		return nil, Revision(0), fmt.Errorf("watch ID must be non-zero")
	}

	logPosition, err := m.log.GetCurrentRevision(ctx)
	if err != nil {
		return nil, logPosition, fmt.Errorf("failed to get current revision: %w", err)
	}

	// logPosition := Revision(req.StartRevision)
	watchDoneRevision := Revision(req.StartRevision)
	if watchDoneRevision == 0 {
		// No start revision == now
		watchDoneRevision = logPosition
	} else {
		// The start revision is inclusive, so we need to subtract 1
		watchDoneRevision--
	}

	w := &memoryWatcher{
		id:                watcherID,
		req:               req,
		logPosition:       logPosition,
		watchDoneRevision: watchDoneRevision,
		storage:           m,

		callback: callback,
	}

	w.stateCond = sync.NewCond(&w.stateMutex)

	if len(w.req.GetRangeEnd()) > 0 {
		if bytes.Equal(w.req.GetRangeEnd(), []byte{0}) {
			w.rangeType = RangeTypeRangeNoEnd
		} else {
			w.rangeType = RangeTypeRangeNormal
		}
	} else {
		w.rangeType = RangeTypeKey
	}

	// if len(watcher.rangeEnd) > 0 {
	// 	watcher.has = true
	// 	if bytes.Equal(watcher.rangeEnd, []byte{0}) {
	// 		watcher.rangeEnd = nilnil
	// 	}
	// }

	m.watchers = append(m.watchers, w)

	// // Start a goroutine to clean up the watcher when the context is done
	// go func() {
	// 	select {
	// 	case <-ctx.Done():
	// 		watcher.Close()
	// 		m.removeWatcher(watcherID)
	// 	case <-watcher.closeCh:
	// 		m.removeWatcher(watcherID)
	// 	}
	// }()

	return w, logPosition, nil
}

// memoryWatcher implements the Watcher interface
type memoryWatcher struct {
	id int64

	req *etcdserverpb.WatchCreateRequest

	rangeType RangeType

	storage *MemoryStorage

	callback func(event *etcdserverpb.WatchResponse) error

	watchDoneRevision Revision

	stateMutex sync.Mutex
	stateCond  *sync.Cond
	// logPosition is the current max position of the log.  It is guarded by stateMutex.
	logPosition Revision
	// closed is true if the watch is stopped.  It is guarded by stateMutex.
	closed bool
}

func (w *memoryWatcher) ID() int64 {
	return w.id
}

func (w *memoryWatcher) Close() {
	w.stateMutex.Lock()
	defer w.stateMutex.Unlock()

	w.closed = true
	w.stateCond.Broadcast()
}

// Run is the main goroutine for the watcher.
func (w *memoryWatcher) Run(ctx context.Context) error {
	log := klog.FromContext(ctx)

	for {
		w.stateMutex.Lock()
		for w.logPosition <= w.watchDoneRevision {
			if err := ctx.Err(); err != nil {
				log.Error(err, "context error in watcher", "watcher.id", w.id)
				w.stateMutex.Unlock()
				return err
			}
			if w.closed {
				log.Info("watcher closed", "watcher.id", w.id)
				w.stateMutex.Unlock()
				return nil
			}
			w.stateCond.Wait()
		}
		logPosition := w.logPosition
		w.stateMutex.Unlock()

		for {
			if w.watchDoneRevision >= logPosition {
				break
			}

			nextRevision := w.watchDoneRevision + 1
			if err := w.send(ctx, nextRevision); err != nil {
				log.Error(err, "failed to send event in watcher", "watcher.id", w.id, "watchPosition", nextRevision)
				return err
			}
			w.watchDoneRevision = nextRevision
		}
	}
}

func (w *memoryWatcher) send(ctx context.Context, pos Revision) error {
	// log := klog.FromContext(ctx)

	// Check if this watcher should receive this event

	events, err := w.storage.getWatchEvent(pos)
	if err != nil {
		return err
	}

	var sendEvents []*mvccpb.Event
	for _, event := range events {
		shouldSend := true
		for _, filter := range w.req.GetFilters() {
			switch filter {
			case etcdserverpb.WatchCreateRequest_NOPUT:
				if event.Type == mvccpb.PUT {
					shouldSend = false
				}
			case etcdserverpb.WatchCreateRequest_NODELETE:
				if event.Type == mvccpb.DELETE {
					shouldSend = false
				}
			default:
				panic("unknown filter")
			}
		}

		if !shouldSend {
			continue
		}

		switch w.rangeType {
		case RangeTypeKey:
			if !bytes.Equal(event.Kv.Key, w.req.GetKey()) {
				shouldSend = false
			}
		case RangeTypeRangeNormal:
			if bytes.Compare(event.Kv.Key, w.req.GetKey()) < 0 || bytes.Compare(event.Kv.Key, w.req.GetRangeEnd()) >= 0 {
				shouldSend = false
			}
		case RangeTypeRangeNoEnd:
			if bytes.Compare(event.Kv.Key, w.req.GetKey()) < 0 {
				shouldSend = false
			}
		default:
			panic("unknown range type")
		}

		if !shouldSend {
			// log.Info("key does not match", "key", string(event.Kv.Key), "rangeEnd", string(w.req.GetRangeEnd()), "rangeType", w.rangeType, "rangeStart", string(w.req.GetKey()))
			continue
		}

		sendEvents = append(sendEvents, event)
	}

	if len(sendEvents) == 0 {
		return nil
	}

	// log.Info("sending events for watcher", "watcher.key", w.req.GetKey(), "watcher.rangeEnd", w.req.GetRangeEnd(), "watcher.filters", w.req.GetFilters(), "watcher.prevKv", w.req.PrevKv, "watcher.id", w.id, "events", sendEvents)

	if !w.req.PrevKv {
		sendEventsWithoutPrevKv := make([]*mvccpb.Event, 0, len(sendEvents))
		for _, event := range sendEvents {
			eventWithoutPrevKv := &mvccpb.Event{
				Type: event.Type,
				Kv:   event.Kv,
			}

			sendEventsWithoutPrevKv = append(sendEventsWithoutPrevKv, eventWithoutPrevKv)
		}
		sendEvents = sendEventsWithoutPrevKv
	}

	resp := &etcdserverpb.WatchResponse{
		Header:  createHeader(pos),
		WatchId: w.id,
		Events:  sendEvents,
	}

	// klog.Infof("broadcasting event %v to watcher %d", resp, w.id)
	if err := w.callback(resp); err != nil {
		return err
	}

	return nil
}

func (w *MemoryStorage) getWatchEvent(pos Revision) ([]*mvccpb.Event, error) {
	logEntry, err := w.log.GetLogEntry(pos)
	if err != nil {
		return nil, err
	}
	if logEntry == nil {
		return nil, fmt.Errorf("log entry not found for position %d", pos)
	}

	return logEntry.Events, nil
}
