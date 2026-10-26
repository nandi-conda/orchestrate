package dev.orchestrate.app;

import android.content.Context;
import android.util.Base64;

import com.chaquo.python.PyObject;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;

import java.security.SecureRandom;

/**
 * The control plane in this app's process (orchestrate_android.py), on a
 * thread of its own, as the desktop window runs it. There is one per process:
 * the activity can be made again (a new window size, a theme change) while it
 * keeps running, and it stops when the activity is finished for good.
 */
final class ControlPlane {
    static final int PORT = 47800;
    static final String BASE_URL = "http://127.0.0.1:" + PORT;
    static final String COOKIE = "orchestrate_session";

    private static ControlPlane instance;

    /** This run's secret: the cookie the WebView and downloads carry. */
    final String secret;
    private PyObject server;
    private Thread thread;

    private ControlPlane() {
        byte[] b = new byte[32];
        new SecureRandom().nextBytes(b);
        secret = Base64.encodeToString(b, Base64.URL_SAFE | Base64.NO_WRAP | Base64.NO_PADDING);
    }

    static synchronized ControlPlane get() {
        if (instance == null) {
            instance = new ControlPlane();
        }
        return instance;
    }

    /** Starts it with these keys unless it is running; a stopping one is waited for first. */
    synchronized void start(Context context, Keys keys) {
        if (thread != null && thread.isAlive() && server != null) {
            return;
        }
        Thread previous = thread;
        if (!Python.isStarted()) {
            Python.start(new AndroidPlatform(context));
        }
        PyObject module = Python.getInstance().getModule("orchestrate_android");
        for (Keys.Key k : Keys.ALL) {
            module.callAttr("set_secret", k.name, keys.get(k.name));
        }
        PyObject next = module.callAttr("ControlPlane", PORT, secret,
                context.getApplicationInfo().nativeLibraryDir);
        server = next;
        thread = new Thread(() -> {
            if (previous != null) {
                try {
                    previous.join();
                } catch (InterruptedException e) {
                    return;
                }
            }
            next.callAttr("run");
        }, "control-plane");
        thread.start();
    }

    synchronized boolean started() {
        return server != null && server.callAttr("started").toBoolean();
    }

    /** Why it stopped, once its thread has ended; null while it runs. */
    synchronized String stopped() {
        if (thread == null || thread.isAlive()) {
            return null;
        }
        return server == null ? "" : server.get("error").toString();
    }

    /** Asks it to stop: its SSE streams end and uvicorn shuts down on its own thread. */
    synchronized void stop() {
        if (server != null) {
            server.callAttr("stop");
            server = null;
        }
    }
}
