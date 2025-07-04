// Package archive provides a persistent ring-buffer cache that stores fixed-size
// records on disk with optional memory-mapping, sharding, buffer pooling, and
// automatic read-ahead (prefetch).
//
// The library is organised into several files for clarity:
//
//	options.go      – configuration struct & defaults
//	shard.go        – shard representation
//	cache.go        – constructors & core fields
//	shard_lookup.go – helper to locate a shard for an ID
//	buffer.go       – pooled buffer & lock helpers
//	io.go           – read/write logic & CRC integrity
//	stats.go        – lightweight stats accessors
//	flush_close.go  – flush & close helpers
//
// See the README for usage examples.
package archive
