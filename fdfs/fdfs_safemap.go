package main

import (
	"sync"
)

type safeMap struct {
	items map[interface{}]interface{}
	sync.RWMutex
}

// Creates a new safe map
func newsafeMap() *safeMap {
	return &safeMap{items: make(map[interface{}]interface{})}
}

// Sets the given value under the specified key.
func (m *safeMap) Set(key interface{}, value interface{}) {
	m.Lock()
	defer m.Unlock()
	m.items[key] = value
}

func (m *safeMap) Reset() {
	m.Lock()
	defer m.Unlock()
	m.items = make(map[interface{}]interface{})
}

func (m *safeMap) Remove(key interface{}) interface{} {
	if v, ok := m.Get(key); ok {
		m.Lock()
		defer m.Unlock()
		delete(m.items, key)
		return v
	}
	return nil
}

func (m *safeMap) SetIfNotExist(key interface{}, value interface{}) (interface{}, bool) {
	m.Lock()
	defer m.Unlock()
	if val, ok := m.items[key]; ok {
		return val, false
	}
	m.items[key] = value
	return value, true
}

// Retrieves an element from map under given key.
func (m *safeMap) Get(key interface{}) (interface{}, bool) {
	m.RLock()
	defer m.RUnlock()

	val, ok := m.items[key]
	return val, ok
}

func (m *safeMap) IterAllKeys() []interface{} {
	m.RLock()
	defer m.RUnlock()
	keys := make([]interface{}, 0, 100)
	for k, _ := range m.items {
		keys = append(keys, k)
	}
	return keys
}

func (m *safeMap) Cover(value map[interface{}]interface{}) {
	m.Lock()
	defer m.Unlock()
	m.items = value
}
