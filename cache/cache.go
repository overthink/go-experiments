package main

import (
	"errors"
	"sync"
	"time"
)

type node struct {
	key   string
	value string
	atime time.Time
}

type LRUCache struct {
	maxSize int
	data    map[string]node
	mutex   sync.Mutex
}

func NewLRUCache(maxSize int) LRUCache {
	return LRUCache{maxSize: maxSize, data: make(map[string]node)}
}

func (c *LRUCache) Get(key string) (string, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	n, exists := c.data[key]
	if !exists {
		return "", false
	}
	n.atime = time.Now()
	c.data[key] = n
	return n.value, true
}

// O(n) scan for smallest atime
func (c *LRUCache) evict() {
	// delete least recently used entry
	var victim node
	for _, n := range c.data {
		if victim.atime.IsZero() || n.atime.Before(victim.atime) {
			victim = n
		}
	}
	delete(c.data, victim.key)
}

func (c *LRUCache) Put(key, value string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.maxSize <= 0 {
		return errors.New("maxSize <= 0")
	}
	_, exists := c.data[key]
	if !exists && len(c.data) == c.maxSize {
		c.evict()
	}
	c.data[key] = node{key, value, time.Now()}
	return nil
}

func (c *LRUCache) Len() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.data)
}
