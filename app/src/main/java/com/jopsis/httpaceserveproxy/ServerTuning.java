package com.jopsis.httpaceserveproxy;

import android.content.Context;
import android.content.SharedPreferences;

/** User-tunable server settings (proxy timeouts, reconnects, engine live buffer). */
public final class ServerTuning {
    private static final String PREFS = "hap_server_tuning";
    private static final String KEY_CLIENT_TIMEOUT = "client_timeout_s";
    private static final String KEY_VIDEO_TIMEOUT = "video_timeout_s";
    private static final String KEY_RECONNECTS = "reconnect_attempts";
    private static final String KEY_LIVE_BUFFER = "live_buffer_s";

    public static final int DEFAULT_CLIENT_TIMEOUT = 60;
    public static final int DEFAULT_VIDEO_TIMEOUT = 60;
    public static final int DEFAULT_RECONNECTS = 5;
    public static final int DEFAULT_LIVE_BUFFER = 0; // 0 = engine default

    private ServerTuning() {
    }

    private static SharedPreferences prefs(Context context) {
        return context.getApplicationContext().getSharedPreferences(PREFS, Context.MODE_PRIVATE);
    }

    public static int clientTimeout(Context c) { return prefs(c).getInt(KEY_CLIENT_TIMEOUT, DEFAULT_CLIENT_TIMEOUT); }
    public static int videoTimeout(Context c) { return prefs(c).getInt(KEY_VIDEO_TIMEOUT, DEFAULT_VIDEO_TIMEOUT); }
    public static int reconnectAttempts(Context c) { return prefs(c).getInt(KEY_RECONNECTS, DEFAULT_RECONNECTS); }
    public static int liveBuffer(Context c) { return prefs(c).getInt(KEY_LIVE_BUFFER, DEFAULT_LIVE_BUFFER); }

    /** Saves clamped values; returns true if the engine must restart (live buffer changed). */
    public static boolean save(Context c, int clientTimeout, int videoTimeout, int reconnects, int liveBuffer) {
        boolean engineChanged = clamp(liveBuffer, 0, 300) != liveBuffer(c);
        prefs(c).edit()
                .putInt(KEY_CLIENT_TIMEOUT, clamp(clientTimeout, 5, 600))
                .putInt(KEY_VIDEO_TIMEOUT, clamp(videoTimeout, 5, 600))
                .putInt(KEY_RECONNECTS, clamp(reconnects, 0, 50))
                .putInt(KEY_LIVE_BUFFER, clamp(liveBuffer, 0, 300))
                .commit();
        return engineChanged;
    }

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }
}
