# Redis-Like In-Memory Key-Value Store (C++)

A lightweight Redis-inspired in-memory database server written in C++23 using raw sockets and a custom RESP (Redis Serialization Protocol) parser.  
It supports a variety of basic Redis commands, list operations, transactions, and streams.  
Runs on **Linux**.

---

## 🚀 Features Implemented

### **Core Capabilities**
- **TCP server on port 6379** — accepts multiple concurrent client connections via threads.
- **RESP protocol parsing** — supports arrays, bulk strings, integers, and error messages.
- **In-memory key-value store** — implemented using `unordered_map`.
- **Optional expiration** — keys can have millisecond-based expiry via `SET key value PX <ms>`.

---

### **Supported Commands**

#### **Connection & String Commands**
- `PING` → returns `PONG`
- `ECHO <message>` → returns the same message
- `SET <key> <value> [PX <ms>]` → set key with optional expiry
- `GET <key>` → get key’s value or `(nil)` if missing/expired
- `INCR <key>` → increments integer value (creates key if missing)

---

#### **Transactions**
- `MULTI` → start transaction (queue commands)
- `EXEC` → execute queued commands atomically
- `DISCARD` → abort transaction

---

#### **List Commands**
- `RPUSH key value [value ...]` → append values to list
- `LPUSH key value [value ...]` → prepend values to list
- `LRANGE key start stop` → return a sublist
- `LLEN key` → list length
- `LPOP key [count]` → remove & return elements from head
- `BLPOP key timeout` → blocking pop from head (with optional timeout)

---

#### **Stream Commands**
- `XADD key id field value [field value ...]`  
  - Supports auto-generated IDs (`*`), partially auto IDs (`<ms>-*`), and explicit IDs
- `XRANGE key start end` → fetch entries in ID range
- `XREAD [BLOCK <ms>] STREAMS key id [key id ...]`  
  - Reads from one or more streams, optionally blocking until new data arrives

---

#### **Utility Commands**
- `TYPE key` → return type: `string`, `list`, `stream`, or `none`
- `KEYS pattern` → returns all keys (only supports `*` wildcard)
- `CONFIG GET <param>` → supports `dir` and `dbfilename`

---

### **Persistence Support**
- On startup, attempts to load an **RDB file** (`dump.rdb` by default) from a configurable directory.
- RDB parsing supports:
  - Strings
  - Integers
  - Expiration times (seconds & milliseconds)
  - Metadata sections

---

### **Blocking Behavior**
- `BLPOP` and `XREAD BLOCK` use `condition_variable` to suspend clients until data is available or timeout expires.
- Cleans up blocked clients on disconnect.

---

### **Limitations**
- Single-threaded command execution per client (one thread per connection).
- No RDB saving — only loading on startup is implemented.
- Minimal pattern matching in `KEYS` (only `*` supported).
- No authentication or ACLs.
- No clustering, replication, or pub/sub.

---

## 📦 Requirements
- **Linux OS** (tested on Ubuntu/Debian)
- `g++` (C++17 or newer, C++23 features used)
- CMake 3.13+
- [vcpkg](https://github.com/microsoft/vcpkg) for dependencies

---

## ⚙️ Installation

### 1. Install Dependencies
```bash
# Install vcpkg (if not already installed)
git clone https://github.com/microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh

# Install packages
./vcpkg/vcpkg install asio pthreads
