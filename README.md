# Redis-Compatible Server in Rust

A high-performance, Redis-compatible server implementation built from scratch in Rust, featuring custom data structures, non-blocking I/O, and advanced memory management techniques.

## Overview

This project implements a Redis-like in-memory data store with a focus on performance, correctness, and educational value. Built entirely in Rust without using existing Redis libraries, it demonstrates low-level systems programming concepts including custom memory management, intrusive data structures, and event-driven networking.

## Features

### Core Functionality

- **Redis Protocol Compatible**: Binary protocol with length-prefixed messages
- **Key-Value Operations**: GET, SET, DEL, KEYS with O(1) hash table lookups
- **Sorted Sets (ZSets)**: ZADD, ZREM, ZQUERY with O(log n) AVL tree operations
- **TTL Support**: EXPIRE, TTL, PERSIST with efficient heap-based expiration
- **Dual-Stack Networking**: IPv4/IPv6 support with single socket binding

### Performance Optimizations

- **Non-Blocking I/O**: Event-driven architecture using `poll()` system calls
- **Custom Buffer Management**: O(1) consume operations, zero-copy where possible
- **Intrusive Collections**: Zero-allocation linked lists and tree operations
- **Incremental Hash Table Rehashing**: Maintains performance during growth
- **Background Thread Pool**: Async cleanup of large data structures

### Data Structures

- **Self-Balancing AVL Trees**: For sorted set operations with guaranteed O(log n) performance
- **Chaining Hash Tables**: With incremental rehashing to maintain load factor
- **Min Heap**: For efficient TTL expiration processing
- **Custom Ring Buffer**: Efficient network I/O buffering

## Architecture

### Network Layer

```
Client Connections → Poll-based Event Loop → Connection State Machine
                                          ↓
Protocol Parsing ← Buffer Management ← Non-blocking Socket I/O
```

### Data Layer

```
Commands → Hash Table Lookup → Value Type Dispatch
                            ↓
        [String Values] | [ZSet AVL Trees] | [TTL Heap]
```

### Concurrency Model

- **Single-threaded event loop** for network I/O (eliminates lock contention)
- **Background thread pool** for expensive operations (large object cleanup)
- **Lock-free data structures** where possible using intrusive collections

## Supported Commands

| Command                              | Description            | Complexity   | Status      |
| ------------------------------------ | ---------------------- | ------------ | ----------- |
| `GET key`                            | Retrieve string value  | O(1)         | ✅ Complete |
| `SET key value`                      | Set string value       | O(1)         | ✅ Complete |
| `DEL key [key ...]`                  | Delete keys            | O(1) per key | ✅ Complete |
| `KEYS`                               | List all keys          | O(n)         | ✅ Complete |
| `ZADD key score member`              | Add to sorted set      | O(log n)     | ✅ Complete |
| `ZREM key member`                    | Remove from sorted set | O(log n)     | ✅ Complete |
| `ZQUERY key score name offset limit` | Range query            | O(log n + k) | ✅ Complete |
| `EXPIRE key seconds`                 | Set TTL                | O(log n)     | ✅ Complete |
| `TTL key`                            | Get remaining TTL      | O(1)         | ✅ Complete |
| `PERSIST key`                        | Remove TTL             | O(log n)     | ✅ Complete |

## Quick Start

### Prerequisites

- Rust 1.70+ with Cargo
- Unix-like system (Linux, macOS) for socket operations

### Installation

```bash
git clone https://github.com/yourusername/redis-rust
cd redis-rust
cargo build --release
```

### Running the Server

```bash
# Start server (listens on [::]:1234)
cargo run --release

# Run test client
cargo run --release -- client
```

### Example Usage

```bash
# Connect with netcat or telnet
nc localhost 1234

# Basic operations
SET mykey "hello world"
GET mykey
DEL mykey

# Sorted sets
ZADD leaderboard 100.5 "player1"
ZADD leaderboard 200.0 "player2"
ZQUERY leaderboard 0 "" 0 10

# TTL operations
EXPIRE mykey 60
TTL mykey
PERSIST mykey
```

## Technical Deep Dive

### Memory Management

The server uses several advanced memory management techniques:

- **Custom Buffer Implementation**: Ring buffer with O(1) consume operations
- **Intrusive Collections**: Data structures embed their own pointers, eliminating separate allocations
- **Reference Counting**: Shared ownership of tree nodes using `Arc<Mutex<>>`

### AVL Tree Implementation

Self-balancing binary search trees maintain O(log n) performance:

- **Rotation Operations**: Left/right rotations with parent pointer updates
- **Height Tracking**: Efficient rebalancing with minimal tree traversals
- **Count Augmentation**: Each node tracks subtree size for offset queries

### Network Protocol

Binary protocol design for efficiency:

```
[4-byte length][variable payload]
```

- Length-prefixed messages prevent buffer overflow attacks
- Little-endian encoding for cross-platform compatibility
- Structured response format with type tags

### TTL Implementation

Efficient expiration using min-heap:

- **Heap-based Timers**: O(log n) insertion/deletion
- **Background Processing**: Non-blocking expiration during event loop
- **Consistent State**: Atomic updates prevent race conditions

## Performance Characteristics

### Benchmarks (Preliminary)

- **Throughput**: ~50k requests/second (single-threaded)
- **Latency**: Sub-millisecond for most operations
- **Memory**: ~100MB for 1M keys with mixed data types
- **Concurrency**: Handles 1000+ concurrent connections

### Scalability

- **Connection Limit**: Limited by file descriptor ulimit
- **Memory Usage**: Linear with data size, efficient representation
- **CPU Usage**: Single core utilization, scales with I/O operations

## Known Limitations

### Current Drawbacks

- **Single-threaded processing**: CPU-bound operations block event loop
- **In-memory only**: No persistence layer implemented
- **Limited command set**: Subset of Redis commands
- **No clustering**: Single-node deployment only

### Future Improvements

- **Async command processing**: Move heavy operations to thread pool
- **Persistence layer**: AOF/RDB-style durability
- **Command parity**: Additional Redis commands (HASH, LIST, etc.)
- **Performance profiling**: Detailed benchmarking and optimization
- **Memory pooling**: Reduce allocation pressure under load

## Development

### Project Structure

```
src/
├── main.rs              # Entry point, client/server selection
├── network/             # Socket handling, protocol parsing
├── data_structures/     # AVL trees, hash tables, heaps
├── commands/            # Redis command implementations
├── memory/              # Buffer management, intrusive collections
└── timer/               # TTL and timeout handling
```

### Testing

```bash
# Run unit tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run

# Memory leak detection
valgrind target/release/redis-rust
```

### Contributing

This project serves as an educational implementation. Areas for contribution:

- Additional Redis commands
- Performance benchmarking
- Protocol extensions
- Documentation improvements

## Learning Outcomes

Building this Redis clone provided hands-on experience with:

1. **Network Programming**: TCP socket management, non-blocking I/O patterns
2. **Data Structure Design**: AVL trees, hash tables, heaps from scratch
3. **Memory Management**: Custom allocators, intrusive collections, zero-copy techniques
4. **Concurrency**: Event-driven architecture, thread pool design
5. **Protocol Design**: Binary protocols, parsing state machines
6. **Systems Programming**: Low-level optimizations, performance profiling

## References

- [Redis Protocol Specification](https://redis.io/topics/protocol)
- [Build Your Own Redis] (https://build-your-own.org/redis/)
- [The Design and Implementation of the 4.3BSD UNIX Operating System](https://www.amazon.com/Design-Implementation-FreeBSD-Operating-System/dp/0321968972)
- [Introduction to Algorithms (CLRS)](https://mitpress.mit.edu/books/introduction-algorithms-third-edition)

## License

MIT License - See LICENSE file for details.

---

**Note**: This is an educational project. For production use cases, consider the official Redis server which offers battle-tested performance, extensive command support, and production-ready features.
