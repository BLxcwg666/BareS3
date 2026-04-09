package storage

import "sync"

type syncEventHub struct {
	mu          sync.Mutex
	nextID      int
	subscribers map[int]chan SyncEvent
}

type syncSettingsHub struct {
	mu          sync.Mutex
	nextID      int
	subscribers map[int]chan SyncSettings
}

func newSyncEventHub() *syncEventHub {
	return &syncEventHub{subscribers: make(map[int]chan SyncEvent)}
}

func newSyncSettingsHub() *syncSettingsHub {
	return &syncSettingsHub{subscribers: make(map[int]chan SyncSettings)}
}

func (h *syncEventHub) subscribe(buffer int) (int, <-chan SyncEvent) {
	if buffer <= 0 {
		buffer = 1
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	id := h.nextID
	channel := make(chan SyncEvent, buffer)
	h.subscribers[id] = channel
	return id, channel
}

func (h *syncEventHub) unsubscribe(id int) {
	h.mu.Lock()
	channel, ok := h.subscribers[id]
	if ok {
		delete(h.subscribers, id)
	}
	h.mu.Unlock()
	if ok {
		close(channel)
	}
}

func (h *syncEventHub) publish(event SyncEvent) {
	h.mu.Lock()
	targets := make([]chan SyncEvent, 0, len(h.subscribers))
	for _, channel := range h.subscribers {
		targets = append(targets, channel)
	}
	h.mu.Unlock()
	for _, channel := range targets {
		select {
		case channel <- event:
		default:
		}
	}
}

func (h *syncSettingsHub) subscribe(buffer int) (int, <-chan SyncSettings) {
	if buffer <= 0 {
		buffer = 1
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	id := h.nextID
	channel := make(chan SyncSettings, buffer)
	h.subscribers[id] = channel
	return id, channel
}

func (h *syncSettingsHub) unsubscribe(id int) {
	h.mu.Lock()
	channel, ok := h.subscribers[id]
	if ok {
		delete(h.subscribers, id)
	}
	h.mu.Unlock()
	if ok {
		close(channel)
	}
}

func (h *syncSettingsHub) publish(settings SyncSettings) {
	h.mu.Lock()
	targets := make([]chan SyncSettings, 0, len(h.subscribers))
	for _, channel := range h.subscribers {
		targets = append(targets, channel)
	}
	h.mu.Unlock()
	for _, channel := range targets {
		select {
		case channel <- settings:
		default:
		}
	}
}

func (s *Store) SubscribeSyncEvents(buffer int) (int, <-chan SyncEvent) {
	return s.syncEvents.subscribe(buffer)
}

func (s *Store) UnsubscribeSyncEvents(id int) {
	s.syncEvents.unsubscribe(id)
}

func (s *Store) SubscribeSyncSettings(buffer int) (int, <-chan SyncSettings) {
	return s.syncSettings.subscribe(buffer)
}

func (s *Store) UnsubscribeSyncSettings(id int) {
	s.syncSettings.unsubscribe(id)
}
