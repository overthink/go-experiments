package main

import (
	"container/list"
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
	data    map[string]*list.Element
	list    *list.List // list of *node; front is LRU element, back is MRU
	mutex   sync.Mutex
}

func NewLRUCache(maxSize int) LRUCache {
	// Our doubly linked list starts and ends with a sentinel node
	// that is never deleted.
	return LRUCache{
		maxSize: maxSize,
		data:    make(map[string]*list.Element),
		list:    list.New(),
	}
}

func (c *LRUCache) Get(key string) (string, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	el, exists := c.data[key]
	if !exists {
		return "", false
	}
	n := el.Value.(*node)
	n.atime = time.Now()
	c.list.MoveToBack(el)
	return n.value, true
}

// constant time: just remove head of list
func (c *LRUCache) evict() {
	if c.list.Len() == 0 {
		return
	}
	victimNode := c.list.Remove(c.list.Front()).(*node)
	delete(c.data, victimNode.key)
}

// O(n) scan for smallest atime
func (c *LRUCache) evictLinear() {
	// delete least recently used entry
	var victim *list.Element
	for _, el := range c.data {
		if victim == nil || el.Value.(*node).atime.Before(victim.Value.(*node).atime) {
			victim = el
		}
	}
	delete(c.data, victim.Value.(*node).key)
}

func (c *LRUCache) Put(key, value string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.maxSize <= 0 {
		return errors.New("maxSize <= 0")
	}
	el, exists := c.data[key]
	if !exists && len(c.data) == c.maxSize {
		c.evict()
	}
	if exists {
		el.Value.(*node).value = value
		el.Value.(*node).atime = time.Now()
		c.list.MoveToBack(el)
	} else {
		el = c.list.PushBack(&node{key, value, time.Now()})
		c.data[key] = el
	}
	return nil
}

func (c *LRUCache) Len() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.data)
}
