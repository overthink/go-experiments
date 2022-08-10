package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypicalPutGet(t *testing.T) {
	c := NewLRUCache(3)
	assert.NoError(t, c.Put("k1", "v1"))
	v, exists := c.Get("k1")
	assert.True(t, exists)
	assert.Equal(t, "v1", v)

	assert.NoError(t, c.Put("k1", "v1.2"))
	v, exists = c.Get("k1")
	assert.True(t, exists)
	assert.Equal(t, "v1.2", v)

	assert.NoError(t, c.Put("k2", "v2"))
	v, exists = c.Get("k2")
	assert.True(t, exists)
	assert.Equal(t, "v2", v)

	assert.Equal(t, 2, c.Len())

	_, exists = c.Get("missing key")
	assert.False(t, exists)
}

func (c *LRUCache) MustGet(key string) string {
	val, exists := c.Get(key)
	if !exists {
		panic("required key not in cache")
	}
	return val
}

func TestEviction(t *testing.T) {
	c := NewLRUCache(3)
	c.Put("k1", "v1")
	c.Put("k2", "v2")
	c.Put("k3", "v3")
	assert.Equal(t, 3, c.Len())

	c.Put("k4", "v4")
	assert.Equal(t, 3, c.Len())
	_, exists := c.Get("k1")
	assert.False(t, exists)
	assert.Equal(t, "v2", c.MustGet("k2"))
	assert.Equal(t, "v3", c.MustGet("k3"))
	assert.Equal(t, "v4", c.MustGet("k4"))

	c.Put("k4", "v4.1")
}

func TestAddingExistingKeyDoesNotEvict(t *testing.T) {
	c := NewLRUCache(2)
	c.Put("k1", "v1")
	c.Put("k2", "v2")
	c.Put("k2", "v2.1") // should not trigger eviction: same key
	assert.Equal(t, 2, c.Len())
	assert.Equal(t, "v1", c.MustGet("k1"))
	assert.Equal(t, "v2.1", c.MustGet("k2"))
}

func TestSizeZeroCache(t *testing.T) {
	c := NewLRUCache(0)
	assert.Error(t, c.Put("k1", "v1"))
}
