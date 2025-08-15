#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <thread>
#include <vector>
#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <chrono>
#include <queue>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <fstream>
#include <sstream>

using namespace std;

// Simple RESP parser: parse a single RESP array of bulk-strings from `req`
bool parse_redis_command(const string &req, vector<string> &out) { // O(n)
    size_t i = 0;
    if (i >= req.size() || req[i] != '*') return false;
    i++;

    size_t start = i;
    while (i < req.size() && req[i] != '\r') i++;
    if (i + 1 >= req.size() || req[i] != '\r' || req[i+1] != '\n') return false;
    string numStr = req.substr(start, i - start);
    i += 2;

    int elements = 0;
    try {
        elements = stoi(numStr);
    } catch (...) { return false; }
    if (elements < 0) return false;

    out.clear();
    out.reserve(elements);

    for (int e = 0; e < elements; ++e) {
        if (i >= req.size() || req[i] != '$') return false;
        i++;
        start = i;
        while (i < req.size() && req[i] != '\r') i++;
        if (i + 1 >= req.size() || req[i] != '\r' || req[i+1] != '\n') return false;
        string lenStr = req.substr(start, i - start);
        i += 2;
        int len = 0;
        try {
            len = stoi(lenStr);
        } catch (...) { return false; }
        if (len < 0) {
            // Null bulk string
            out.push_back(string());
            continue;
        }
        if ((size_t)len > req.size() - i) return false;
        string data = req.substr(i, len);
        i += len;
        if (i + 1 >= req.size() || req[i] != '\r' || req[i+1] != '\n') return false;
        i += 2;
        out.push_back(move(data));
    }
    return true;
}

void send_simple_string(int fd, const string &s) {
    string out = "+" + s + "\r\n";
    send(fd, out.c_str(), out.size(), 0);
}

void send_error(int fd, const string &msg) {
    string out = "-" + msg + "\r\n";
    send(fd, out.c_str(), out.size(), 0);
}

void send_bulk_string(int client_fd, const string &value) {
    string resp = "$" + to_string(value.size()) + "\r\n" + value + "\r\n";
    send(client_fd, resp.c_str(), resp.size(), 0);
}

// Data store and lists
unordered_map<string, string> store;
unordered_map<string, chrono::steady_clock::time_point> expiry;
// <-- changed to deque for efficient pop_front/push_front
unordered_map<string, deque<string>> lists;

// RDB configuration parameters
string rdb_dir = "/tmp";
string rdb_filename = "dump.rdb";

// Transaction state tracking
unordered_map<int, bool> transaction_state; // client_fd -> in_transaction
unordered_map<int, vector<vector<string>>> transaction_queues; // client_fd -> queue of commands

// Blocked client struct and global blocked map
struct BlockedClient {
    int fd;
    string key;
    string value;
    bool ready = false;
    mutex mtx; // Protects shared data inside the struct from being accessed/modified by multiple threads at the same time.
    condition_variable cv; // Allows one thread to wait until another thread signals that something happened.
};
struct BlockedXReadClient {
    int fd;
    vector<string> keys;
    vector<pair<long long,long long>> start_ids; // ms, seq per stream
    bool ready = false;
    vector<pair<string, vector<pair<string, unordered_map<string,string>>>>> results;
    mutex mtx;
    condition_variable cv;
};
unordered_map<string, queue<shared_ptr<BlockedXReadClient>>> blocked_xread_clients;
unordered_map<string, queue<shared_ptr<BlockedClient>>> blocked_clients;
mutex global_mtx; // protects lists and blocked_clients
unordered_map<string, vector<pair<string, unordered_map<string, string>>>> streams;

// RDB file reading functions
uint64_t read_size_encoding(ifstream& file) {
    unsigned char first_byte;
    file.read(reinterpret_cast<char*>(&first_byte), 1);
    
    uint8_t encoding_type = (first_byte >> 6) & 0x03;
    
    switch (encoding_type) {
        case 0: // 0b00: 6 bits
            return first_byte & 0x3F;
        case 1: { // 0b01: 14 bits
            uint16_t value = (first_byte & 0x3F) << 8;
            unsigned char second_byte;
            file.read(reinterpret_cast<char*>(&second_byte), 1);
            value |= second_byte;
            return value;
        }
        case 2: { // 0b10: 32 bits
            uint32_t value = 0;
            unsigned char bytes[4];
            file.read(reinterpret_cast<char*>(bytes), 4);
            for (int i = 0; i < 4; i++) {
                value = (value << 8) | bytes[i];
            }
            return value;
        }
        case 3: { // 0b11: special encoding
            // For special encoding, return the first byte as-is
            // The string encoding function will handle the special encoding
            return first_byte;
        }
        default:
            throw runtime_error("Invalid size encoding");
    }
}

string read_string_encoding(ifstream& file) {
    uint64_t length = read_size_encoding(file);
    
    // Check if this is a special encoding (0b11)
    if ((length & 0xC0) == 0xC0) {
        // This is a special encoding, not a regular string
        uint8_t encoding_type = length & 0x3F;
        
        switch (encoding_type) {
            case 0: { // 8-bit integer
                uint8_t value;
                file.read(reinterpret_cast<char*>(&value), 1);
                return to_string(value);
            }
            case 1: { // 16-bit integer (little-endian)
                uint16_t value;
                file.read(reinterpret_cast<char*>(&value), 2);
                return to_string(value);
            }
            case 2: { // 32-bit integer (little-endian)
                uint32_t value;
                file.read(reinterpret_cast<char*>(&value), 4);
                return to_string(value);
            }
            case 3: // LZF compressed
                throw runtime_error("LZF compression not supported");
            default:
                throw runtime_error("Invalid special encoding");
        }
    }
    
    // Regular string
    string result(length, '\0');
    file.read(&result[0], length);
    return result;
}

void load_rdb_file() {
    string filepath = rdb_dir + "/" + rdb_filename;
    ifstream file(filepath, ios::binary);
    
    if (!file.is_open()) {
        return;
    }
    
    // Read header: REDIS0011
    char header[9];
    file.read(header, 9);
    if (string(header, 9) != "REDIS0011") {
        throw runtime_error("Invalid RDB header");
    }
    
    // Skip metadata section and find database section
    unsigned char byte;
    bool in_database_section = false;
    
    while (file.read(reinterpret_cast<char*>(&byte), 1)) {
        if (byte == 0xFA) { // Metadata subsection
            // Skip metadata name and value
            read_string_encoding(file); // metadata name
            read_string_encoding(file); // metadata value
        } else if (byte == 0xFE) { // Database subsection
            in_database_section = true;
            break;
        } else if (byte == 0xFF) { // End of file
            return;
        } else {
            // This might be the start of key-value pairs without a database marker
            file.seekg(-1, ios::cur); // Go back one byte
            break;
        }
    }
    
    // Read database subsection if we found one
    if (in_database_section) {
        uint64_t db_index = read_size_encoding(file);
        
        // Read hash table size info
        unsigned char next_byte;
        if (file.read(reinterpret_cast<char*>(&next_byte), 1)) {
            if (next_byte == 0xFB) {
                uint64_t hash_table_size = read_size_encoding(file);
                uint64_t expires_size = read_size_encoding(file);
            } else {
                file.seekg(-1, ios::cur); // Go back one byte
            }
        }
    }
    
    // Read key-value pairs
    while (file.read(reinterpret_cast<char*>(&byte), 1)) {
        if (byte == 0xFF) { // End of file
            break;
        } else if (byte == 0xFE) { // Another database
            break;
        }
        
        // Check for expire information
        chrono::steady_clock::time_point expire_time = chrono::steady_clock::time_point::min();
        if (byte == 0xFD) { // Expire in seconds
            uint32_t expire_seconds;
            file.read(reinterpret_cast<char*>(&expire_seconds), 4);
            auto now = chrono::system_clock::now();
            auto expire_point = chrono::system_clock::from_time_t(expire_seconds);
            expire_time = chrono::steady_clock::now() + (expire_point - now);
            file.read(reinterpret_cast<char*>(&byte), 1); // Read value type
        } else if (byte == 0xFC) { // Expire in milliseconds
            uint64_t expire_ms;
            file.read(reinterpret_cast<char*>(&expire_ms), 8);
            auto now = chrono::system_clock::now();
            auto expire_point = chrono::system_clock::from_time_t(expire_ms / 1000) + 
                              chrono::milliseconds(expire_ms % 1000);
            expire_time = chrono::steady_clock::now() + (expire_point - now);
            file.read(reinterpret_cast<char*>(&byte), 1); // Read value type
        }
        
        // Read key and value
        try {
            string key = read_string_encoding(file);
            string value = read_string_encoding(file);
            
            // Store in memory
            store[key] = value;
            if (expire_time != chrono::steady_clock::time_point::min()) {
                expiry[key] = expire_time;
            }
        } catch (const exception& e) {
            break;
        }
    }
}

string to_upper(const string &s) {
    string result = s;
    transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

// Remove any blocked entries for a disconnected fd (simple O(N) cleanup)
void cleanup_blocked_client_fd(int client_fd) {
    lock_guard<mutex> g(global_mtx);
    for (auto &p : blocked_clients) {
        queue<shared_ptr<BlockedClient>> tmp;
        while (!p.second.empty()) {
            auto bc = p.second.front();
            p.second.pop();
            if (bc && bc->fd != client_fd) tmp.push(bc);
            // if bc->fd == client_fd, drop it
        }
        p.second = move(tmp);
    }
    
    // Also cleanup blocked XREAD clients
    for (auto &p : blocked_xread_clients) {
        queue<shared_ptr<BlockedXReadClient>> tmp;
        while (!p.second.empty()) {
            auto bxc = p.second.front();
            p.second.pop();
            if (bxc && bxc->fd != client_fd) tmp.push(bxc);
            // if bxc->fd == client_fd, drop it
        }
        p.second = move(tmp);
    }
    
    // Cleanup transaction state
    transaction_state.erase(client_fd);
    transaction_queues.erase(client_fd);
    
}

bool parse_id(const string &id, long long &ms, long long &seq) {
    size_t dash_pos = id.find('-');
    if (dash_pos == string::npos) return false;
    try {
        ms = stoll(id.substr(0, dash_pos));
        seq = stoll(id.substr(dash_pos + 1));
    } catch (...) {
        return false;
    }
    return true;
}

void execute_redis_command(int client_fd, const vector<string> &cmd) {
    if (cmd.empty()) return;
    string command = to_upper(cmd[0]);

    // Check if client is in a transaction state
    bool in_transaction = false;
    {
        lock_guard<mutex> lock(global_mtx);
        in_transaction = (transaction_state.find(client_fd) != transaction_state.end());
    }

    // If in transaction and not MULTI/EXEC/DISCARD, queue the command
    if (in_transaction && command != "MULTI" && command != "EXEC" && command != "DISCARD") {
        lock_guard<mutex> lock(global_mtx);
        transaction_queues[client_fd].push_back(cmd);
        send(client_fd, "+QUEUED\r\n", 9, 0);
        return;
    }

    if (command == "PING") {
        send(client_fd, "+PONG\r\n", 7, 0);
    }

    else if (command == "ECHO" && cmd.size() >= 2) {
        send_bulk_string(client_fd, cmd[1]);
    }

    else if (command == "SET" && cmd.size() >= 3) {
        string key = cmd[1];
        string value = cmd[2];
        {
            lock_guard<mutex> lock(global_mtx);
            store[key] = value;
            expiry.erase(key);
            if (cmd.size() >= 5 && to_upper(cmd[3]) == "PX") {
                try {
                    long ms = stol(cmd[4]);
                    expiry[cmd[1]] = chrono::steady_clock::now() + chrono::milliseconds(ms);
                } catch (...) {
                    string err = "-ERR PX value is not an integer\r\n";
                    send(client_fd, err.c_str(), err.size(), 0);
                    return;
                }
            }
        }
        send(client_fd, "+OK\r\n", 5, 0);
    }

    else if (command == "GET" && cmd.size() >= 2) {
        string key = cmd[1];
        lock_guard<mutex> lock(global_mtx);
        auto it = store.find(key);
        if (it == store.end()) {
            send(client_fd, "$-1\r\n", 5, 0);
            return;
        }
        auto exp_it = expiry.find(key);
        if (exp_it != expiry.end() && chrono::steady_clock::now() > exp_it->second) {
            store.erase(it);
            expiry.erase(exp_it);
            send(client_fd, "$-1\r\n", 5, 0);
            return;
        }
        send_bulk_string(client_fd, it->second);
    }

    else if (command == "MULTI") {
        // MULTI command starts a transaction
        lock_guard<mutex> lock(global_mtx);
        transaction_state[client_fd] = true;
        transaction_queues[client_fd].clear(); // Clear any existing queue
        send(client_fd, "+OK\r\n", 5, 0);
    }

    else if (command == "EXEC") {
        // EXEC command executes all commands queued in a transaction
        lock_guard<mutex> lock(global_mtx);
        auto it = transaction_state.find(client_fd);
        if (it == transaction_state.end()) {
            // No active transaction
            send_error(client_fd, "ERR EXEC without MULTI");
        } else {
            // Get the queued commands
            auto queue_it = transaction_queues.find(client_fd);
            if (queue_it == transaction_queues.end() || queue_it->second.empty()) {
                // No commands queued, return empty array
                transaction_state.erase(it);
                transaction_queues.erase(client_fd);
                send(client_fd, "*0\r\n", 4, 0);
            } else {
                // Execute all queued commands and collect responses
                vector<string> responses;
                for (const auto &queued_cmd : queue_it->second) {
                    // Create a temporary string to capture the response
                    string response;
                    
                    // We need to capture the response from each command
                    // For now, we'll execute them directly and capture the response
                    // This is a simplified approach - in a real implementation,
                    // we'd want to capture the actual RESP responses
                    
                    // Execute the command and capture its response
                    string cmd_name = to_upper(queued_cmd[0]);
                    
                    if (cmd_name == "SET" && queued_cmd.size() >= 3) {
                        string key = queued_cmd[1];
                        string value = queued_cmd[2];
                        store[key] = value;
                        expiry.erase(key);
                        if (queued_cmd.size() >= 5 && to_upper(queued_cmd[3]) == "PX") {
                            try {
                                long ms = stol(queued_cmd[4]);
                                expiry[key] = chrono::steady_clock::now() + chrono::milliseconds(ms);
                            } catch (...) {
                                // Ignore PX errors in transaction
                            }
                        }
                        responses.push_back("+OK");
                    }
                    else if (cmd_name == "GET" && queued_cmd.size() >= 2) {
                        string key = queued_cmd[1];
                        auto store_it = store.find(key);
                        if (store_it == store.end()) {
                            responses.push_back("$-1");
                        } else {
                            auto exp_it = expiry.find(key);
                            if (exp_it != expiry.end() && chrono::steady_clock::now() > exp_it->second) {
                                store.erase(store_it);
                                expiry.erase(exp_it);
                                responses.push_back("$-1");
                            } else {
                                responses.push_back("$" + to_string(store_it->second.size()) + "\r\n" + store_it->second);
                            }
                        }
                    }
                    else if (cmd_name == "INCR" && queued_cmd.size() == 2) {
                        string key = queued_cmd[1];
                        auto store_it = store.find(key);
                        if (store_it == store.end()) {
                            store[key] = "1";
                            responses.push_back(":1");
                        } else {
                            auto exp_it = expiry.find(key);
                            if (exp_it != expiry.end() && chrono::steady_clock::now() > exp_it->second) {
                                store.erase(store_it);
                                expiry.erase(exp_it);
                                store[key] = "1";
                                responses.push_back(":1");
                            } else {
                                try {
                                    long long current_value = stoll(store_it->second);
                                    long long new_value = current_value + 1;
                                    store[key] = to_string(new_value);
                                    responses.push_back(":" + to_string(new_value));
                                } catch (...) {
                                    responses.push_back("-ERR value is not an integer or out of range");
                                }
                            }
                        }
                    }
                    else {
                        // Unknown command in transaction
                        responses.push_back("-ERR unknown command");
                    }
                }
                
                // Build the RESP array response
                string resp = "*" + to_string(responses.size()) + "\r\n";
                for (const auto &response : responses) {
                    resp += response + "\r\n";
                }
                
                // Clear transaction state
                transaction_state.erase(it);
                transaction_queues.erase(client_fd);
                
                // Send the response
                send(client_fd, resp.c_str(), resp.size(), 0);
            }
        }
    }

    else if (command == "DISCARD") {
        // DISCARD command aborts a transaction by discarding all queued commands
        lock_guard<mutex> lock(global_mtx);
        auto it = transaction_state.find(client_fd);
        if (it == transaction_state.end()) {
            // No active transaction
            send_error(client_fd, "ERR DISCARD without MULTI");
        } else {
            // Abort the transaction by clearing the state and queue
            transaction_state.erase(it);
            transaction_queues.erase(client_fd);
            send(client_fd, "+OK\r\n", 5, 0);
        }
    }

    else if (command == "INCR" && cmd.size() == 2) {
        string key = cmd[1];
        lock_guard<mutex> lock(global_mtx);
        
        // Check if key exists
        auto it = store.find(key);
        if (it == store.end()) {
            // Key doesn't exist - set it to 1
            store[key] = "1";
            string resp = ":1\r\n";
            send(client_fd, resp.c_str(), resp.size(), 0);
            return;
        }
        
        // Check for expiry
        auto exp_it = expiry.find(key);
        if (exp_it != expiry.end() && chrono::steady_clock::now() > exp_it->second) {
            store.erase(it);
            expiry.erase(exp_it);
            // Key expired, treat as if it doesn't exist - set it to 1
            store[key] = "1";
            string resp = ":1\r\n";
            send(client_fd, resp.c_str(), resp.size(), 0);
            return;
        }
        
        // Try to parse the current value as an integer
        long long current_value;
        try {
            current_value = stoll(it->second);
        } catch (...) {
            // For this stage, we only handle numerical values
            // This case will be handled in later stages
            send_error(client_fd, "ERR value is not an integer or out of range");
            return;
        }
        
        // Increment the value
        long long new_value = current_value + 1;
        
        // Update the store with the new value
        store[key] = to_string(new_value);
        
        // Send the response as an integer
        string resp = ":" + to_string(new_value) + "\r\n";
        send(client_fd, resp.c_str(), resp.size(), 0);
    }

    else if (command == "RPUSH") {
      if (cmd.size() < 3) {
          send(client_fd, "-ERR wrong number of arguments for 'rpush' command\r\n", 55, 0);
          return;
      }
      string key = cmd[1];

      // Capture original length (under lock) so we can compute final length deterministically
      size_t original_len = 0;
      {
          lock_guard<mutex> lock(global_mtx);
          original_len = lists[key].size();
      }

      size_t pushed_count = 0; // number of values from this RPUSH
      for (size_t i = 2; i < cmd.size(); ++i) {
          shared_ptr<BlockedClient> bc;

          {
              lock_guard<mutex> lock(global_mtx);
              if (!blocked_clients[key].empty()) {
                  // Serve blocked client (do not append to container)
                  bc = blocked_clients[key].front();
                  blocked_clients[key].pop();
              } else {
                  // No one waiting, append to list (so container state matches)
                  lists[key].push_back(cmd[i]);
              }
          } 

          // If we found a blocked client, wake it with the pushed value
          if (bc) {
              {
                  lock_guard<mutex> lck(bc->mtx);
                  bc->value = cmd[i];
                  bc->ready = true;
              }
              bc->cv.notify_one();
          }

          pushed_count++;
      }

      // According to Redis behaviour expected by the tests, the RPUSH response
      // should be the list length *as if* all pushes occurred: original_len + pushed_count
      size_t final_len = original_len + pushed_count;
      string reply = ":" + to_string(final_len) + "\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
  }


    else if (command == "LPUSH") {
        if (cmd.size() < 3) {
            send(client_fd, "-ERR wrong number of arguments for 'lpush' command\r\n", 55, 0);
            return;
        }
        string key = cmd[1];

        for (size_t i = 2; i < cmd.size(); ++i) {
            shared_ptr<BlockedClient> bc;

            {
                lock_guard<mutex> lock(global_mtx);
                if (!blocked_clients[key].empty()) {
                    bc = blocked_clients[key].front();
                    blocked_clients[key].pop();
                } else {
                    lists[key].push_front(cmd[i]); // use push_front for deque
                }
            }

            if (bc) {
                {
                    lock_guard<mutex> lck(bc->mtx);
                    bc->value = cmd[i];
                    bc->ready = true;
                }
                bc->cv.notify_one();
            }
        }

        size_t length = 0;
        {
            lock_guard<mutex> lock(global_mtx);
            length = lists[key].size();
        }
        string reply = ":" + to_string(length) + "\r\n";
        send(client_fd, reply.c_str(), reply.size(), 0);
    }

    else if (command == "LRANGE") {
        if (cmd.size() != 4) {
            send(client_fd, "-ERR wrong number of arguments for 'lrange' command\r\n", 55, 0);
            return;
        }
        string key = cmd[1];

        // Take snapshot inside lock
        deque<string> snapshot;
        {
            lock_guard<mutex> lock(global_mtx);
            auto it = lists.find(key);
            if (it == lists.end()) {
                send(client_fd, "*0\r\n", 4, 0);
                return;
            }
            snapshot = it->second; // copy for safe iteration outside of further modifications
        }

        int start = 0, stop = 0;
        try {
            start = stoi(cmd[2]);
            stop  = stoi(cmd[3]);
        } catch (...) {
            send_error(client_fd, "ERR value is not an integer");
            return;
        }

        int len = (int)snapshot.size();
        if (start < 0) start = len + start;
        if (start < 0) start = 0;
        if (stop < 0) stop = len + stop;
        if (stop < 0) stop = 0;
        if (stop >= len) stop = len - 1;
        if (start > stop || start >= len) {
            send(client_fd, "*0\r\n", 4, 0);
            return;
        }

        int count = stop - start + 1;
        string resp = "*" + to_string(count) + "\r\n";
        for (int i = start; i <= stop; ++i) {
            resp += "$" + to_string(snapshot[i].size()) + "\r\n" + snapshot[i] + "\r\n";
        }
        send(client_fd, resp.c_str(), resp.size(), 0);
    }

    else if (command == "LLEN") {
        if (cmd.size() != 2) {
            send(client_fd, "-ERR wrong number of arguments for 'llen' command\r\n", 54, 0);
            return;
        }
        string key = cmd[1];
        size_t length = 0;
        {
            lock_guard<mutex> lock(global_mtx);
            auto it = lists.find(key);
            if (it != lists.end()) length = it->second.size();
        }
        string resp = ":" + to_string(length) + "\r\n";
        send(client_fd, resp.c_str(), resp.size(), 0);
    }

    else if (command == "LPOP") {
        if (cmd.size() < 2) {
            send(client_fd, "-ERR wrong number of arguments for 'lpop' command\r\n", 53, 0);
            return;
        }
        string key = cmd[1];

        lock_guard<mutex> lock(global_mtx);
        if (lists.find(key) == lists.end() || lists[key].empty()) {
            send(client_fd, "$-1\r\n", 5, 0);
            return;
        }

        int count = 1;
        if (cmd.size() >= 3) {
            try {
                count = stoi(cmd[2]);
                if (count <= 0) {
                    send(client_fd, "*0\r\n", 4, 0);
                    return;
                }
            } catch (...) {
                send(client_fd, "-ERR value is not an integer or out of range\r\n", 48, 0);
                return;
            }
        }

        if (count > (int)lists[key].size()) count = lists[key].size();

        if (count == 1 && cmd.size() == 2) {
            string value = lists[key].front();
            lists[key].pop_front();
            string resp = "$" + to_string(value.size()) + "\r\n" + value + "\r\n";
            send(client_fd, resp.c_str(), resp.size(), 0);
            return;
        }

        string resp = "*" + to_string(count) + "\r\n";
        for (int i = 0; i < count; ++i) {
            string value = lists[key].front();
            lists[key].pop_front();
            resp += "$" + to_string(value.size()) + "\r\n" + value + "\r\n";
        }
        send(client_fd, resp.c_str(), resp.size(), 0);
    }

    else if (command == "BLPOP") {
      if (cmd.size() != 3) {
          send(client_fd, "-ERR wrong number of arguments for 'blpop' command\r\n", 55, 0);
          return;
      }
      string key = cmd[1];
      double timeout_seconds = 0;
      try { 
          timeout_seconds = stod(cmd[2]); 
          if (timeout_seconds < 0) timeout_seconds = 0;
      } catch (...) { 
          timeout_seconds = 0; 
      }

      // Fast path: if there's already data, return immediately
      {
          lock_guard<mutex> lock(global_mtx);
          auto it = lists.find(key);
          if (it != lists.end() && !it->second.empty()) {
              string value = it->second.front();
              it->second.pop_front();
              string resp = "*2\r\n$" + to_string(key.size()) + "\r\n" + key + "\r\n" +
                            "$" + to_string(value.size()) + "\r\n" + value + "\r\n";
              send(client_fd, resp.c_str(), resp.size(), 0);
              return;
          }
      }

      // Prepare blocked client
      auto bc = make_shared<BlockedClient>(); //make shared is a generic which constructs 
                                              //an object of type T <T> and returns a shared_ptr pointing to it 
      bc->fd = client_fd;
      bc->key = key;

      {
          lock_guard<mutex> lock(global_mtx);
          blocked_clients[key].push(bc);
      }

      unique_lock<mutex> lk(bc->mtx); //blocked client's mutex so it waits independently
                                      // Its a felxible RAII lock for mutexes

      if (timeout_seconds <= 0) {
          // Block indefinitely
          bc->cv.wait(lk, [&]{ return bc->ready; });
      } else {
          // Block for timeout_seconds
          auto timeout_duration = chrono::milliseconds((long long)(timeout_seconds * 1000));
          bool woke_up = bc->cv.wait_for(lk, timeout_duration, [&]{ return bc->ready; });

          if (!woke_up) {
              // Timeout: remove from blocked_clients if still there
              {
                  lock_guard<mutex> lock(global_mtx);
                  // rebuild queue without bc
                  queue<shared_ptr<BlockedClient>> new_q;
                  while (!blocked_clients[key].empty()) {
                      auto front_bc = blocked_clients[key].front();
                      blocked_clients[key].pop();
                      if (front_bc != bc) {
                          new_q.push(front_bc);
                      }
                  }
                  blocked_clients[key] = std::move(new_q);
              }
              string null_reply = "$-1\r\n";
              send(client_fd, null_reply.c_str(), null_reply.size(), 0);
              return;
          }
      }

      // Send the result once woken
      string resp = "*2\r\n$" + to_string(key.size()) + "\r\n" + key + "\r\n" +
                    "$" + to_string(bc->value.size()) + "\r\n" + bc->value + "\r\n";
      send(client_fd, resp.c_str(), resp.size(), 0);
    }

    else if (command == "TYPE") {
      if (cmd.size() != 2) {
          send(client_fd, "-ERR wrong number of arguments for 'type' command\r\n", 53, 0);
          return;
      }

      string key = cmd[1];
      string type = "none";

      {
          lock_guard<mutex> lock(global_mtx);

          if (store.find(key) != store.end()) {
              type = "string";
          } else if (lists.find(key) != lists.end() && !lists[key].empty()) {
              type = "list";
          } else if (streams.find(key) != streams.end() && !streams[key].empty()) {
              type = "stream";
          }
      }

      string reply = "+" + type + "\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
    }

    else if (command == "XADD") {
        if (cmd.size() < 5 || (cmd.size() - 3) % 2 != 0) {
            const char* err_msg = "-ERR wrong number of arguments for 'xadd' command\r\n";
            send(client_fd, err_msg, strlen(err_msg), 0);
            return;
        }

        string key = cmd[1];
        string id  = cmd[2];

        long long ms = 0, seq = 0;
        bool auto_seq = false;
        bool auto_time_and_seq = false;
        bool id_is_auto = (id.find('*') != string::npos); // Track if ID is partially auto

        if (id == "*") {
            auto_time_and_seq = true;
            id_is_auto = true;
        } else {
            size_t dash_pos = id.find('-');
            if (dash_pos == string::npos) {
                const char* err_msg = "-ERR Invalid ID format\r\n";
                send(client_fd, err_msg, strlen(err_msg), 0);
                return;
            }
            string ms_part  = id.substr(0, dash_pos);
            string seq_part = id.substr(dash_pos + 1);

            try {
                ms = stoll(ms_part);
            } catch (...) {
                const char* err_msg = "-ERR Invalid ID format\r\n";
                send(client_fd, err_msg, strlen(err_msg), 0);
                return;
            }

            if (seq_part == "*") {
                auto_seq = true; // ms fixed, seq auto
                id_is_auto = true;
            } else {
                try {
                    seq = stoll(seq_part);
                } catch (...) {
                    const char* err_msg = "-ERR Invalid ID format\r\n";
                    send(client_fd, err_msg, strlen(err_msg), 0);
                    return;
                }
            }
        }

        string final_id;
        {
            lock_guard<mutex> lock(global_mtx);
            auto &entries = streams[key];

            if (auto_time_and_seq) {
                // Auto ms + auto seq
                auto now = chrono::system_clock::now();
                auto ms_since_epoch = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count();
                ms = ms_since_epoch;
                seq = -1;
                for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
                    long long last_ms, last_seq;
                    if (!parse_id(it->first, last_ms, last_seq)) continue;
                    if (last_ms == ms) { seq = last_seq; break; }
                    if (last_ms < ms) break;
                }
                if (seq == -1) {
                    seq = (ms == 0) ? 1 : 0;
                } else {
                    seq += 1;
                }
                final_id = to_string(ms) + "-" + to_string(seq);
            }
            else if (auto_seq) {
                // ms is fixed, seq auto
                seq = -1;
                for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
                    long long last_ms, last_seq;
                    if (!parse_id(it->first, last_ms, last_seq)) continue;
                    if (last_ms == ms) { seq = last_seq; break; }
                    if (last_ms < ms) break;
                }
                if (seq == -1) {
                    seq = (ms == 0) ? 1 : 0;
                } else {
                    seq += 1;
                }
                final_id = to_string(ms) + "-" + to_string(seq);
            }
            else {
                // Fully explicit ID
                final_id = id;
            }
            // Ordering validation
            if (!entries.empty()) {
                long long last_ms, last_seq;
                parse_id(entries.back().first, last_ms, last_seq);

                // Special case: if requested ID is 0-0, always return specific error
                if (ms == 0 && seq == 0) {
                    string err_msg = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                    send(client_fd, err_msg.c_str(), err_msg.size(), 0);
                    return;
                }

                // Improved check: true if id is "*" or ends with "-*"
                bool id_is_partial_auto = (id == "*" || (id.size() > 2 && id.substr(id.size() - 2) == "-*"));

                if (ms < last_ms || (ms == last_ms && seq <= last_seq)) {
                    if (id_is_partial_auto) {
                        string err_msg = "-ERR The ID specified in XADD must be greater than " + entries.back().first + "\r\n";
                        send(client_fd, err_msg.c_str(), err_msg.size(), 0);
                    } else {
                        string err_msg = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                        send(client_fd, err_msg.c_str(), err_msg.size(), 0);
                    }
                    return;
                }
            }
            unordered_map<string, string> fields;
            for (size_t i = 3; i < cmd.size(); i += 2) {
                fields[cmd[i]] = cmd[i + 1];
            }
            entries.push_back({final_id, fields});

            // ===== Wake up blocked XREAD clients =====
            auto bxit = blocked_xread_clients.find(key);
            if (bxit != blocked_xread_clients.end()) {
                queue<shared_ptr<BlockedXReadClient>> remaining;
                while (!bxit->second.empty()) {
                    auto bxc = bxit->second.front();
                    bxit->second.pop();

                    int idx = -1;
                    for (int i = 0; i < (int)bxc->keys.size(); ++i) {
                        if (bxc->keys[i] == key) { idx = i; break; }
                    }
                    if (idx == -1) continue;

                    vector<pair<string, unordered_map<string, string>>> matches;
                    // Only check the new entry that was just added (the last entry)
                    if (!entries.empty()) {
                        auto &new_entry = entries.back();
                        long long ems, eseq;
                        if (parse_id(new_entry.first, ems, eseq)) {
                            auto [start_ms, start_seq] = bxc->start_ids[idx];
                            if ((ems > start_ms) || (ems == start_ms && eseq > start_seq)) {
                                matches.push_back(new_entry);
                            }
                        }
                    }

                    if (!matches.empty()) {
                        bxc->results.push_back({key, matches});
                        bxc->ready = true;
                        bxc->cv.notify_one();
                    } else {
                        remaining.push(bxc);
                    }
                }
                bxit->second = move(remaining);
            }
        }

        string reply = "$" + to_string(final_id.size()) + "\r\n" + final_id + "\r\n";
        send(client_fd, reply.c_str(), reply.size(), 0);
    }

    else if (command == "XRANGE") {
        if (cmd.size() != 4) {
            send(client_fd, "-ERR wrong number of arguments for 'xrange' command\r\n", 55, 0);
            return;
        }

        const string key   = cmd[1];
        const string start = cmd[2];
        const string end   = cmd[3];

        // Parse "ms[-seq]" with default seq if omitted.
        auto parse_with_default = [](const string& id, long long default_seq,
                                    long long& ms_out, long long& seq_out) -> bool {
            size_t dash = id.find('-');
            try {
                if (dash == string::npos) {
                    ms_out  = stoll(id);
                    seq_out = default_seq;
                } else {
                    ms_out  = stoll(id.substr(0, dash));
                    seq_out = stoll(id.substr(dash + 1));
                }
                return true;
            } catch (...) {
                return false;
            }
        };

        const long long MAX_SEQ = 0x7fffffffffffffffLL; // avoid <limits> include
        long long start_ms = 0, start_seq = 0, end_ms = 0, end_seq = 0;

        // Support '-' for start => beginning of stream
        if (start == "-") {
            start_ms = 0;
            start_seq = 0;
        } else {
            if (!parse_with_default(start, 0LL, start_ms, start_seq)) {
                send_error(client_fd, "ERR Invalid ID format");
                return;
            }
        }

        // Support '+' for end => end of stream
        if (end == "+") {
            end_ms = MAX_SEQ;
            end_seq = MAX_SEQ;
        } else {
            if (!parse_with_default(end, MAX_SEQ, end_ms, end_seq)) {
                send_error(client_fd, "ERR Invalid ID format");
                return;
            }
        }

        // Snapshot the stream under lock
        vector<pair<string, unordered_map<string, string>>> snapshot;
        {
            lock_guard<mutex> lock(global_mtx);
            auto it = streams.find(key);
            if (it == streams.end()) {
                send(client_fd, "*0\r\n", 4, 0);
                return;
            }
            snapshot = it->second;
        }

        // Collect matches (inclusive range)
        vector<pair<string, unordered_map<string, string>>> matches;
        for (auto &entry : snapshot) {
            long long ems, eseq;
            if (!parse_id(entry.first, ems, eseq)) continue; // be safe
            bool ge_start = (ems > start_ms) || (ems == start_ms && eseq >= start_seq);
            bool le_end   = (ems < end_ms)   || (ems == end_ms   && eseq <= end_seq);
            if (ge_start && le_end) matches.push_back(entry);
        }

        // Build RESP response: array of [id, [field, value, ...]]
        string resp = "*" + to_string(matches.size()) + "\r\n";
        for (auto &e : matches) {
            resp += "*2\r\n";
            // ID
            resp += "$" + to_string(e.first.size()) + "\r\n" + e.first + "\r\n";
            // Field-value list
            resp += "*" + to_string(e.second.size() * 2) + "\r\n";
            for (const auto &kv : e.second) {
                resp += "$" + to_string(kv.first.size()) + "\r\n"  + kv.first  + "\r\n";
                resp += "$" + to_string(kv.second.size()) + "\r\n" + kv.second + "\r\n";
            }
        }
        send(client_fd, resp.c_str(), resp.size(), 0);
    }

    else if (command == "KEYS" && cmd.size() == 2) {
        // KEYS command returns all keys matching the pattern
        string pattern = cmd[1];
        vector<string> matching_keys;
        
        {
            lock_guard<mutex> lock(global_mtx);
            for (const auto& pair : store) {
                // For this stage, we only support "*" pattern (all keys)
                if (pattern == "*") {
                    matching_keys.push_back(pair.first);
                }
            }
        }
        
        // Build RESP array response
        string resp = "*" + to_string(matching_keys.size()) + "\r\n";
        for (const auto& key : matching_keys) {
            resp += "$" + to_string(key.size()) + "\r\n" + key + "\r\n";
        }
        send(client_fd, resp.c_str(), resp.size(), 0);
    }

    else if (command == "CONFIG" && cmd.size() >= 3 && to_upper(cmd[1]) == "GET") {
        // CONFIG GET command returns configuration parameter values
        string param = to_upper(cmd[2]);
        
        if (param == "DIR") {
            string resp = "*2\r\n$3\r\ndir\r\n$" + to_string(rdb_dir.size()) + "\r\n" + rdb_dir + "\r\n";
            send(client_fd, resp.c_str(), resp.size(), 0);
        }
        else if (param == "DBFILENAME") {
            string resp = "*2\r\n$10\r\ndbfilename\r\n$" + to_string(rdb_filename.size()) + "\r\n" + rdb_filename + "\r\n";
            send(client_fd, resp.c_str(), resp.size(), 0);
        }
        else {
            // Unknown parameter - return empty array
            send(client_fd, "*0\r\n", 4, 0);
        }
    }

    else if (command == "XREAD") {
        bool blocking = false;
        long long block_ms = 0;

        int streams_index = -1;
        if (cmd.size() >= 4 && to_upper(cmd[1]) == "BLOCK") {
            blocking = true;
            try { block_ms = stoll(cmd[2]); } catch (...) {
                send_error(client_fd, "ERR Invalid block timeout");
                return;
            }
            if (cmd.size() >= 5 && to_upper(cmd[3]) == "STREAMS") {
                streams_index = 3;
            }
        } else if (to_upper(cmd[1]) == "STREAMS") {
            streams_index = 1;
        }

        if (streams_index == -1) {
            send_error(client_fd, "ERR syntax error");
            return;
        }

        int num_streams = (cmd.size() - (streams_index + 1)) / 2;
        if ((int)cmd.size() != (streams_index + 1) + num_streams * 2) {
            send_error(client_fd, "ERR wrong number of arguments for 'xread'");
            return;
        }

        vector<string> keys;
        vector<pair<long long,long long>> start_ids;
        keys.reserve(num_streams);
        start_ids.reserve(num_streams);

        for (int i = 0; i < num_streams; ++i) {
            keys.push_back(cmd[streams_index + 1 + i]);
        }
        for (int i = 0; i < num_streams; ++i) {
            string id_str = cmd[streams_index + 1 + num_streams + i];
            if (id_str == "$") {
                // Special case: $ means "only new entries"
                // We'll set this to the maximum ID in the stream, or 0-0 if stream is empty
                long long max_ms = 0, max_seq = 0;
                {
                    lock_guard<mutex> lock(global_mtx);
                    auto it = streams.find(keys[i]);
                    if (it != streams.end() && !it->second.empty()) {
                        // Find the maximum ID in the stream
                        for (const auto &entry : it->second) {
                            long long ems, eseq;
                            if (parse_id(entry.first, ems, eseq)) {
                                if (ems > max_ms || (ems == max_ms && eseq > max_seq)) {
                                    max_ms = ems;
                                    max_seq = eseq;
                                }
                            }
                        }
                    }
                }
                start_ids.push_back({max_ms, max_seq});
            } else {
                long long ms, seq;
                if (!parse_id(id_str, ms, seq)) {
                    send_error(client_fd, "ERR Invalid ID format");
                    return;
                }
                start_ids.push_back({ms, seq});
            }
        }

        // Fast path: check for immediate matches
        vector<pair<string, vector<pair<string, unordered_map<string,string>>>>> results;
        {
            lock_guard<mutex> lock(global_mtx);
            for (int i = 0; i < num_streams; ++i) {
                auto it = streams.find(keys[i]);
                if (it == streams.end()) continue;
                vector<pair<string, unordered_map<string,string>>> matches;
                for (auto &entry : it->second) {
                    long long ems, eseq;
                    if (!parse_id(entry.first, ems, eseq)) continue;
                    if ((ems > start_ids[i].first) || (ems == start_ids[i].first && eseq > start_ids[i].second)) {
                        matches.push_back(entry);
                    }
                }
                if (!matches.empty()) {
                    results.push_back({keys[i], matches});
                }
            }
        }

        if (!results.empty()) {
            string resp = "*" + to_string(results.size()) + "\r\n";
            for (auto &stream_pair : results) {
                const string &stream_name = stream_pair.first;
                const auto &entries = stream_pair.second;
                resp += "*2\r\n";
                resp += "$" + to_string(stream_name.size()) + "\r\n" + stream_name + "\r\n";
                resp += "*" + to_string(entries.size()) + "\r\n";
                for (auto &e : entries) {
                    resp += "*2\r\n";
                    resp += "$" + to_string(e.first.size()) + "\r\n" + e.first + "\r\n";
                    resp += "*" + to_string(e.second.size() * 2) + "\r\n";
                    for (auto &kv : e.second) {
                        resp += "$" + to_string(kv.first.size()) + "\r\n" + kv.first + "\r\n";
                        resp += "$" + to_string(kv.second.size()) + "\r\n" + kv.second + "\r\n";
                    }
                }
            }
            send(client_fd, resp.c_str(), resp.size(), 0);
            return;
        }

        if (!blocking) {
            send(client_fd, "*0\r\n", 4, 0);
            return;
        }

        // Prepare blocked client
        auto bxc = make_shared<BlockedXReadClient>();
        bxc->fd = client_fd;
        bxc->keys = keys;
        bxc->start_ids = start_ids;

        {
            lock_guard<mutex> lock(global_mtx);
            for (auto &k : keys) {
                blocked_xread_clients[k].push(bxc);
            }
        }

        unique_lock<mutex> lk(bxc->mtx);
        if (block_ms <= 0) {
            bxc->cv.wait(lk, [&]{ return bxc->ready; });
        } else {
            if (!bxc->cv.wait_for(lk, chrono::milliseconds(block_ms), [&]{ return bxc->ready; })) {
                string null_reply = "$-1\r\n";
                send(client_fd, null_reply.c_str(), null_reply.size(), 0);
                return;
            }
        }

        // If we reach here, bxc->ready is true and we should have results
        // If results are empty, it means no new entries were found, so return null
        if (bxc->results.empty()) {
            string null_reply = "$-1\r\n";
            send(client_fd, null_reply.c_str(), null_reply.size(), 0);
            return;
        }

        string resp = "*" + to_string(bxc->results.size()) + "\r\n";
        for (auto &stream_pair : bxc->results) {
            const string &stream_name = stream_pair.first;
            const auto &entries = stream_pair.second;
            resp += "*2\r\n";
            resp += "$" + to_string(stream_name.size()) + "\r\n" + stream_name + "\r\n";
            resp += "*" + to_string(entries.size()) + "\r\n";
            for (auto &e : entries) {
                resp += "*2\r\n";
                resp += "$" + to_string(e.first.size()) + "\r\n" + e.first + "\r\n";
                resp += "*" + to_string(e.second.size() * 2) + "\r\n";
                for (auto &kv : e.second) {
                    resp += "$" + to_string(kv.first.size()) + "\r\n" + kv.first + "\r\n";
                    resp += "$" + to_string(kv.second.size()) + "\r\n" + kv.second + "\r\n";
                }
            }
        }
                 send(client_fd, resp.c_str(), resp.size(), 0);
     }
     
     else {
         send(client_fd, "-ERR unknown command\r\n", 23, 0);
     }
}

void handle_client(int client_fd) {
    char buffer[4096];
    while (true) {
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received == 0) {
            cerr << "Client disconnected\n";
            cleanup_blocked_client_fd(client_fd);
            break;
        } else if (bytes_received < 0) {
            cerr << "recv error\n";
            cleanup_blocked_client_fd(client_fd);
            break;
        }
        string req(buffer, buffer + bytes_received);
        cout << "Received " << bytes_received << " bytes\n";
        vector<string> parsed;
        bool ok = parse_redis_command(req, parsed);
        if (!ok) {
            send_error(client_fd, "ERR Protocol error: malformed request");
            continue;
        }
        execute_redis_command(client_fd, parsed);
    }
    close(client_fd);
}

int main(int argc, char **argv) {
    cout << unitbuf;
    cerr << unitbuf;

    // Parse command line arguments for RDB configuration
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "--dir" && i + 1 < argc) {
            rdb_dir = argv[i + 1];
            i++; // Skip the next argument since we consumed it
        } else if (arg == "--dbfilename" && i + 1 < argc) {
            rdb_filename = argv[i + 1];
            i++; // Skip the next argument since we consumed it
        }
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        cerr << "Failed to create server socket\n";
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
        cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        cerr << "listen failed\n";
        return 1;
    }

    cout << "Server listening on port 6379\n";

    // Load data from RDB file on startup
    try {
        load_rdb_file();
    } catch (const exception& e) {
        // Continue running even if RDB loading fails
    }

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_fd < 0) {
            cerr << "Accept failed\n";
            continue;
        }
        cout << "New client connected, spawning thread...\n";
        thread t(handle_client, client_fd);
        t.detach();
    }

    close(server_fd);
    return 0;
}
