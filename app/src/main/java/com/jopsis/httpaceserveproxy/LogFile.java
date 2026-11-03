package com.jopsis.httpaceserveproxy;

import android.content.ClipData;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;

import androidx.core.content.FileProvider;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

/**
 * Persists the full HaP runtime log (HaP events + AceServe output) to
 * files/logs/hap.log, rotating to hap.log.1 at {@link #MAX_BYTES}. The native
 * proxy writes its own lines to files/logs/proxy.log. The on-screen log keeps
 * only the last few KB; these files keep the whole trace for diagnosis.
 */
public final class LogFile {
    static final long MAX_BYTES = 8L * 1024 * 1024;
    private static final String AUTHORITY_SUFFIX = ".logs";
    private static final SimpleDateFormat TS =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US);

    private static File dir;
    private static File file;
    private static BufferedWriter writer;
    private static long size;

    private LogFile() {
    }

    public static synchronized void init(Context context) {
        if (dir != null) return;
        dir = logsDir(context);
        if (!dir.exists() && !dir.mkdirs()) {
            dir = null;
            return;
        }
        file = new File(dir, "hap.log");
        size = file.length();
    }

    public static File logsDir(Context context) {
        return new File(context.getApplicationContext().getFilesDir(), "logs");
    }

    static synchronized void write(String line) {
        if (file == null) return;
        try {
            if (writer == null) open();
            String out = TS.format(new Date()) + " " + line + "\n";
            writer.write(out);
            writer.flush();
            size += out.length();
            if (size > MAX_BYTES) rotate();
        } catch (IOException e) {
            closeQuietly();
        }
    }

    private static void open() throws IOException {
        writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file, true), StandardCharsets.UTF_8));
        size = file.length();
    }

    private static void rotate() throws IOException {
        closeQuietly();
        File old = new File(dir, "hap.log.1");
        if (old.exists()) old.delete();
        file.renameTo(old);
        open();
    }

    private static void closeQuietly() {
        try {
            if (writer != null) writer.close();
        } catch (IOException ignored) {
        }
        writer = null;
    }

    /** Builds a share intent with every log file that exists, or null if there are none. */
    public static synchronized Intent shareIntent(Context context) {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException ignored) {
            }
        }
        File logs = logsDir(context);
        String[] names = {"hap.log", "hap.log.1", "proxy.log", "proxy.log.1"};
        String authority = context.getPackageName() + AUTHORITY_SUFFIX;
        ArrayList<Uri> uris = new ArrayList<>();
        for (String name : names) {
            File f = new File(logs, name);
            if (f.isFile() && f.length() > 0) uris.add(FileProvider.getUriForFile(context, authority, f));
        }
        if (uris.isEmpty()) return null;
        Intent intent = new Intent(Intent.ACTION_SEND_MULTIPLE);
        intent.setType("text/plain");
        intent.putParcelableArrayListExtra(Intent.EXTRA_STREAM, uris);
        intent.putExtra(Intent.EXTRA_SUBJECT, "HaP logs");
        ClipData clip = ClipData.newRawUri("HaP logs", uris.get(0));
        for (int i = 1; i < uris.size(); i++) clip.addItem(new ClipData.Item(uris.get(i)));
        intent.setClipData(clip);
        intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION);
        return intent;
    }
}
