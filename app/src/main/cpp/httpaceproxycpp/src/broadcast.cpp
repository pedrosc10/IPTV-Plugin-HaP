#include "httpaceproxycpp/broadcast.hpp"
#include "httpaceproxycpp/util.hpp"

#include <algorithm>
#include <chrono>
#include <sstream>
#include <thread>

namespace httpace {

ChunkQueue::ChunkQueue(std::size_t max_chunks) : max_chunks_(std::max<std::size_t>(2, max_chunks)) {}

PushResult ChunkQueue::push(std::vector<char> chunk, std::chrono::milliseconds wait) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) return PushResult::Closed;
    if (chunks_.size() >= max_chunks_ && wait.count() > 0) {
        cv_space_.wait_for(lock, wait, [&] {
            return closed_ || chunks_.size() < max_chunks_;
        });
        if (closed_) return PushResult::Closed;
    }
    PushResult result = PushResult::Ok;
    if (chunks_.size() >= max_chunks_) {
        chunks_.pop_front();
        result = PushResult::DroppedOldest;
    }
    chunks_.push_back(std::move(chunk));
    cv_data_.notify_one();
    return result;
}

bool ChunkQueue::pop(std::vector<char>& chunk) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_data_.wait(lock, [&] { return closed_ || !chunks_.empty(); });
    if (chunks_.empty()) return false;
    chunk = std::move(chunks_.front());
    chunks_.pop_front();
    cv_space_.notify_one();
    return true;
}

void ChunkQueue::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    cv_data_.notify_all();
    cv_space_.notify_all();
}

std::size_t ChunkQueue::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return chunks_.size();
}

Broadcast::Broadcast(std::string infohash, Config config, HttpClient& http_client,
                     std::map<std::string, std::string> start_params)
    : infohash_(std::move(infohash)),
      config_(std::move(config)),
      http_client_(http_client),
      start_params_(std::move(start_params)),
      ace_(std::make_shared<AceClient>(config_, "Broadcast_" + infohash_.substr(0, 8))) {
    ace_->authenticate();
}

Broadcast::~Broadcast() { stop(); }

std::shared_ptr<StreamClient> Broadcast::add_client(const std::string& client_ip,
                                                    const std::string& user_agent,
                                                    const std::string& channel_name,
                                                    const std::string& channel_icon) {
    auto client = std::make_shared<StreamClient>();
    client->session_id = std::to_string(reinterpret_cast<std::uintptr_t>(client.get()));
    client->client_ip = client_ip;
    client->user_agent = user_agent;
    client->channel_name = channel_name;
    client->channel_icon = channel_icon.empty() ? "http://static.acestream.net/sites/acestream/img/ACE-logo.png" : channel_icon;
    client->connection_time = unix_time();
    client->last_activity = client->connection_time;
    client->queue = std::make_shared<ChunkQueue>(static_cast<std::size_t>(std::max(2, config_.client_queue_size)));
    client->ace = ace();
    client->broadcast = weak_from_this();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        clients_.push_back(client);
    }
    return client;
}

void Broadcast::remove_client(const std::shared_ptr<StreamClient>& client) {
    if (!client) return;
    client->queue->close();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        clients_.erase(std::remove_if(clients_.begin(), clients_.end(), [&](const auto& weak) {
            auto locked = weak.lock();
            return !locked || locked == client;
        }), clients_.end());
    }
    if (client_count() == 0) stop();
}

std::size_t Broadcast::client_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t count = 0;
    for (const auto& weak : clients_) if (!weak.expired()) ++count;
    return count;
}

std::vector<std::shared_ptr<StreamClient>> Broadcast::clients() const {
    std::vector<std::shared_ptr<StreamClient>> out;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& weak : clients_) {
        if (auto client = weak.lock()) out.push_back(client);
    }
    return out;
}

void Broadcast::start_once() {
    bool expected = false;
    if (started_.compare_exchange_strong(expected, true)) {
        running_ = true;
        stream_thread_ = std::thread(&Broadcast::stream_loop, this);
    }
}

void Broadcast::stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true)) return;
    running_ = false;
    for (auto& client : clients()) client->queue->close();
    auto ace = swap_ace(nullptr);
    if (ace) {
        try { ace->stop_broadcast(); } catch (...) {}
        try { ace->shutdown(); } catch (...) {}
    }
    if (stream_thread_.joinable() && stream_thread_.get_id() != std::this_thread::get_id()) stream_thread_.join();
}

std::shared_ptr<AceClient> Broadcast::swap_ace(std::shared_ptr<AceClient> next) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto old = ace_;
    ace_ = std::move(next);
    return old;
}

// One engine session: START, then relay the stream until it ends. Returns true
// if any data was relayed.
bool Broadcast::run_attempt() {
    auto ace = this->ace();
    if (!ace) throw std::runtime_error("no engine session");
    any_data_ = false;
    std::atomic<bool> attempt_done{false};
    std::thread keepalive([this, &attempt_done] {
        while (!attempt_done && running_ && !any_data_) {
            send_keepalive();
            for (int i = 0; i < 10 && !attempt_done; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });
    struct JoinGuard {
        std::atomic<bool>& done;
        std::thread& t;
        ~JoinGuard() { done = true; if (t.joinable()) t.join(); }
    } guard{attempt_done, keepalive};
    auto started = ace->start_broadcast(start_params_);
    auto url = rewrite_url_host_port(url_decode(started.url), config_.ace_host, std::to_string(config_.ace_http_port));
    log_line("INFO", "[" + infohash_.substr(0, 8) + "] stream URL " + url);
    if (ends_with(parse_url(url).path, ".m3u8")) stream_hls_url(url);
    else stream_http_url(url);
    return any_data_;
}

// MPEG-TS null packets (PID 0x1FFF): players discard them, but they keep the
// HTTP connection active so the player's read timeout doesn't fire while the
// engine session is (re)started. Only sent before real data flows.
void Broadcast::send_keepalive() {
    static const std::vector<char> null_packets = [] {
        std::vector<char> out;
        for (int i = 0; i < 7; ++i) {
            out.push_back(static_cast<char>(0x47));
            out.push_back(static_cast<char>(0x1F));
            out.push_back(static_cast<char>(0xFF));
            out.push_back(static_cast<char>(0x10));
            out.insert(out.end(), 184, static_cast<char>(0xFF));
        }
        return out;
    }();
    if (ts_output_) broadcast_chunk(null_packets.data(), null_packets.size(), true);
}

void Broadcast::wait_with_keepalive(int seconds) {
    for (int i = 0; i < seconds && running_ && client_count() > 0; ++i) {
        send_keepalive();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void Broadcast::stream_loop() {
    const std::string tag = "[" + infohash_.substr(0, 8) + "] ";
    const int max_attempts = std::max(0, config_.reconnect_attempts);
    int failures = 0;
    while (running_ && client_count() > 0) {
        auto attempt_start = std::chrono::steady_clock::now();
        try {
            run_attempt();
        } catch (const std::exception& e) {
            log_line("ERROR", tag + "stream failed: " + std::string(e.what()));
        }
        if (!running_ || client_count() == 0) break;

        auto lasted = std::chrono::steady_clock::now() - attempt_start;
        if (lasted > std::chrono::seconds(60)) failures = 0;
        if (++failures > max_attempts) {
            log_line("ERROR", tag + "giving up after " + std::to_string(max_attempts) + " reconnect attempts");
            break;
        }
        log_line("WARNING", tag + "engine stream dropped, reconnecting (" + std::to_string(failures) + "/" +
                 std::to_string(max_attempts) + ")");

        auto old = swap_ace(nullptr);
        if (old) {
            try { old->stop_broadcast(); } catch (...) {}
            try { old->shutdown(); } catch (...) {}
        }
        any_data_ = false;
        wait_with_keepalive(std::min(failures * 2, 8));
        if (!running_ || client_count() == 0) break;
        try {
            auto fresh = std::make_shared<AceClient>(config_, "Broadcast_" + infohash_.substr(0, 8));
            fresh->authenticate();
            if (!running_) {
                try { fresh->shutdown(); } catch (...) {}
                break;
            }
            swap_ace(std::move(fresh));
        } catch (const std::exception& e) {
            log_line("ERROR", tag + "engine reconnect failed: " + std::string(e.what()));
        }
    }
    running_ = false;
    for (auto& client : clients()) client->queue->close();
}

void Broadcast::stream_http_url(const std::string& url) {
    http_client_.stream(url, [&](const char* data, std::size_t size) {
        broadcast_chunk(data, size);
        if (!running_ || client_count() == 0) {
            running_ = false;
            return false;
        }
        return true;
    }, running_, 5, config_.video_timeout, std::max(1, config_.curl_stream_buffer));
}

void Broadcast::stream_hls_url(const std::string& url) {
    std::vector<std::string> seen;
    int consecutive_errors = 0;
    while (running_ && client_count() > 0) {
        try {
            auto response = http_client_.get(url, {}, config_.video_timeout);
            auto base = parse_url(url);
            for (const auto& raw_line : split(response.body, '\n', false)) {
                auto line = trim(raw_line);
                if (line.empty() || starts_with(line, "#")) continue;
                std::string segment = line;
                if (!starts_with(segment, "http://") && !starts_with(segment, "https://")) {
                    auto base_path = base.path;
                    auto slash = base_path.find_last_of('/');
                    base_path = slash == std::string::npos ? "/" : base_path.substr(0, slash + 1);
                    segment = base.scheme + "://" + base.authority + base_path + segment;
                }
                if (std::find(seen.begin(), seen.end(), segment) != seen.end()) continue;
                stream_http_url(segment);
                seen.push_back(segment);
                if (seen.size() > 50) seen.erase(seen.begin());
                if (!running_ || client_count() == 0) break;
            }
            consecutive_errors = 0;
        } catch (const std::exception& e) {
            log_line("ERROR", "HLS refresh failed: " + std::string(e.what()));
            // Let stream_loop rebuild the engine session instead of polling a dead playlist forever.
            if (++consecutive_errors >= 5) return;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void Broadcast::broadcast_chunk(const char* data, std::size_t size, bool keepalive) {
    std::lock_guard<std::mutex> chunk_lock(chunk_mutex_);
    if (keepalive) {
        if (any_data_) return;  // never interleave filler with real stream bytes
    } else if (size > 0 && !any_data_.exchange(true) && !ts_output_) {
        ts_output_ = static_cast<unsigned char>(data[0]) == 0x47;
    }
    std::vector<char> chunk(data, data + size);
    auto write_timeout = std::max(1, config_.client_write_timeout);
    // Bound the per-push wait so one slow client can't stall the engine reader for long.
    auto wait = std::chrono::milliseconds(std::min(write_timeout * 1000 / 4, 2000));
    auto now = unix_time();
    for (auto& client : clients()) {
        auto result = client->queue->push(chunk, wait);
        if (result == PushResult::Closed) continue;
        if (result == PushResult::DroppedOldest) {
            client->dropped_chunks.fetch_add(1, std::memory_order_relaxed);
            if (now - client->last_activity.load() > write_timeout) {
                if (!client->stuck_logged.exchange(true)) {
                    log_line("WARNING", "[" + client->client_ip + "] client too slow ("
                             + std::to_string(client->dropped_chunks.load()) + " chunks dropped in "
                             + std::to_string(write_timeout) + "s), closing");
                    client->queue->close();
                }
            }
        } else {
            client->dropped_chunks.store(0, std::memory_order_relaxed);
        }
    }
}

BroadcastManager::BroadcastManager(Config config, HttpClient& http_client)
    : config_(std::move(config)), http_client_(http_client) {}

BroadcastManager::~BroadcastManager() { stop_all(); }

std::shared_ptr<Broadcast> BroadcastManager::get_or_create(const std::string& infohash,
                                                           const std::map<std::string, std::string>& params) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = broadcasts_.find(infohash);
    if (it != broadcasts_.end()) {
        if (!it->second->finished()) return it->second;
        // A dead broadcast never sends data again; replace it instead of
        // attaching the new client to it (that client would hang forever).
        broadcasts_.erase(it);
    }
    auto broadcast = std::make_shared<Broadcast>(infohash, config_, http_client_, params);
    broadcasts_[infohash] = broadcast;
    return broadcast;
}

std::shared_ptr<Broadcast> BroadcastManager::find(const std::string& infohash) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = broadcasts_.find(infohash);
    return it == broadcasts_.end() ? nullptr : it->second;
}

void BroadcastManager::remove_if_empty(const std::string& infohash) {
    std::shared_ptr<Broadcast> removed;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = broadcasts_.find(infohash);
        if (it != broadcasts_.end() && it->second->client_count() == 0) {
            removed = it->second;
            broadcasts_.erase(it);
        }
    }
    if (removed) removed->stop();
}

std::size_t BroadcastManager::broadcast_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return broadcasts_.size();
}

std::size_t BroadcastManager::client_count() const {
    std::size_t total = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [_, broadcast] : broadcasts_) total += broadcast->client_count();
    return total;
}

std::vector<std::shared_ptr<StreamClient>> BroadcastManager::all_clients() const {
    std::vector<std::shared_ptr<StreamClient>> out;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [_, broadcast] : broadcasts_) {
        auto clients = broadcast->clients();
        out.insert(out.end(), clients.begin(), clients.end());
    }
    return out;
}

void BroadcastManager::stop_all() {
    std::map<std::string, std::shared_ptr<Broadcast>> copy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        copy.swap(broadcasts_);
    }
    for (auto& [_, broadcast] : copy) broadcast->stop();
}

} // namespace httpace
