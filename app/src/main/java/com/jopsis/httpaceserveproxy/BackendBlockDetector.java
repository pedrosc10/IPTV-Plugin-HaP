package com.jopsis.httpaceserveproxy;

/**
 * Detects when the ISP blocks the Ace Stream backend (e.g. LaLiga match-time
 * blocking in Spain): content IDs fail with "Cannot load transport file" while
 * the engine's backend requests are refused or answered with 403. Cleared as
 * soon as the engine is downloading a stream again.
 */
final class BackendBlockDetector {
    private static final long BACKEND_ERROR_WINDOW_MS = 120_000;

    static volatile boolean blocked;
    private static long lastBackendErrorAt;

    private BackendBlockDetector() {
    }

    static void onEngineLine(String line) {
        if (line == null) return;
        long now = System.currentTimeMillis();
        if (line.contains("acestream.Service|send_request: http error")
                || (line.contains("BackendMonitor|ping failed") && line.contains("403"))) {
            lastBackendErrorAt = now;
        } else if (line.contains("Cannot load transport file")) {
            if (now - lastBackendErrorAt < BACKEND_ERROR_WINDOW_MS) setBlocked(true);
        } else if (blocked && line.contains("status_string=main:dl")) {
            setBlocked(false);
        }
    }

    static void reset() {
        lastBackendErrorAt = 0;
        if (blocked) setBlocked(false);
    }

    private static synchronized void setBlocked(boolean value) {
        if (blocked == value) return;
        blocked = value;
        if (value) {
            ServiceState.error("Ace Stream backend blocked by the network (403 / connection refused). Turn on a VPN (e.g. Warp) on this device.");
        } else {
            ServiceState.appendLog("backend reachable again");
            if (ServiceState.lastError.startsWith("Ace Stream backend blocked")) ServiceState.error("");
        }
    }
}
