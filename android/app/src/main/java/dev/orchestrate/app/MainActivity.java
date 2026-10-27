package dev.orchestrate.app;

import android.app.Activity;
import android.Manifest;
import android.app.DownloadManager;
import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Insets;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.Looper;
import android.text.InputType;
import android.view.View;
import android.view.ViewGroup;
import android.view.WindowInsets;
import android.webkit.CookieManager;
import android.webkit.JavascriptInterface;
import android.webkit.URLUtil;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceRequest;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;
import android.window.OnBackInvokedCallback;
import android.window.OnBackInvokedDispatcher;

import java.util.HashMap;
import java.util.Map;

/**
 * The orchestrator on the phone: the ultra UI in a WebView, from the control
 * plane running in this app's process (ControlPlane), as the desktop window
 * shows it. Before the first start it asks for the keys the control plane
 * needs (Keys).
 */
public class MainActivity extends Activity {
    private static final int POLL_MS = 100;

    private final Handler main = new Handler(Looper.getMainLooper());
    private Keys keys;
    private WebView web;
    private TextView status;
    // Back steps back through the UI's pages while there are any, then leaves the app.
    private final OnBackInvokedCallback back = () -> {
        if (web != null) {
            web.goBack();
        }
        updateBack();
    };
    private boolean backRegistered;

    private final Runnable watch = new Runnable() {
        @Override
        public void run() {
            ControlPlane cp = ControlPlane.get();
            String why = cp.stopped();
            if (why != null) {
                ControlPlaneService.stop(MainActivity.this);
                showStatus(getString(R.string.stopped, why.isEmpty() ? getString(R.string.no_reason) : why), true);
            } else if (cp.started()) {
                showWeb();
            } else {
                main.postDelayed(this, POLL_MS);
            }
        }
    };

    @Override
    protected void onCreate(Bundle state) {
        super.onCreate(state);
        keys = new Keys(this);
        if (keys.complete()) {
            start();
        } else {
            showKeysForm();
        }
    }

    /** Shows `content` as the whole window, with no toolbar: the UI has its
     *  own header, and a phone's screen is better spent on content. Android
     *  draws apps edge to edge, under the status and navigation bars, and no
     *  longer resizes them for the keyboard, so the window pads itself clear
     *  of all three. */
    private void show(View content) {
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.addView(content, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f));
        root.setOnApplyWindowInsetsListener((view, insets) -> {
            Insets bars = insets.getInsets(WindowInsets.Type.systemBars() | WindowInsets.Type.ime()
                    | WindowInsets.Type.displayCutout());
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom);
            return WindowInsets.CONSUMED;
        });
        setContentView(root);
    }

    private void start() {
        showStatus(getString(R.string.starting), false);
        // Its notification keeps the control plane running off screen (ControlPlaneService).
        if (checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[] {Manifest.permission.POST_NOTIFICATIONS}, 0);
        }
        ControlPlaneService.start(this);
        ControlPlane.get().start(this, keys);
        main.removeCallbacks(watch);
        main.post(watch);
    }

    private void restart() {
        ControlPlane.get().stop();
        start();
    }

    private void showStatus(String text, boolean retry) {
        destroyWeb();
        LinearLayout box = column();
        status = new TextView(this);
        status.setText(text);
        status.setTextIsSelectable(true);
        box.addView(status);
        if (retry) {
            Button again = new Button(this);
            again.setText(R.string.retry);
            again.setOnClickListener(v -> start());
            box.addView(again, wrap());
            Button edit = new Button(this);
            edit.setText(R.string.keys);
            edit.setOnClickListener(v -> showKeysForm());
            box.addView(edit, wrap());
        }
        show(scroll(box));
    }

    private void showKeysForm() {
        main.removeCallbacks(watch);
        destroyWeb();
        LinearLayout box = column();
        TextView title = new TextView(this);
        title.setText(R.string.keys_title);
        title.setTextAppearance(android.R.style.TextAppearance_Material_Headline);
        box.addView(title);
        Map<String, EditText> fields = new HashMap<>();
        for (Keys.Key k : Keys.ALL) {
            TextView label = new TextView(this);
            label.setText(k.name + (k.required ? "" : " " + getString(R.string.optional)));
            label.setPadding(0, dp(16), 0, 0);
            label.setTextAppearance(android.R.style.TextAppearance_Material_Subhead);
            box.addView(label);
            TextView help = new TextView(this);
            help.setText(k.help);
            box.addView(help);
            EditText field = new EditText(this);
            field.setSingleLine(true);
            field.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_PASSWORD);
            field.setText(keys.get(k.name));
            box.addView(field);
            fields.put(k.name, field);
        }
        Button save = new Button(this);
        save.setText(R.string.save_keys);
        save.setOnClickListener(v -> {
            boolean ok = true;
            for (Keys.Key k : Keys.ALL) {
                EditText f = fields.get(k.name);
                if (k.required && f.getText().toString().trim().isEmpty()) {
                    f.setError(getString(R.string.required));
                    ok = false;
                }
            }
            if (!ok) {
                return;
            }
            for (Keys.Key k : Keys.ALL) {
                keys.set(k.name, fields.get(k.name).getText().toString().trim());
            }
            restart();
        });
        box.addView(save, wrap());
        if (keys.complete()) {
            Button cancel = new Button(this);
            cancel.setText(android.R.string.cancel);
            cancel.setOnClickListener(v -> start());
            box.addView(cancel, wrap());
        }
        show(scroll(box));
    }

    private void showWeb() {
        if (web != null) {
            return;
        }
        CookieManager cookies = CookieManager.getInstance();
        cookies.setAcceptCookie(true);
        cookies.setCookie(ControlPlane.BASE_URL,
                ControlPlane.COOKIE + "=" + ControlPlane.get().secret + "; Path=/; HttpOnly; SameSite=Strict");
        web = new WebView(this);
        // The UI's Settings opens the Keys screen through this; only the
        // control plane's own pages load here (links out go to the browser).
        web.addJavascriptInterface(new Host(), "orchestrateApp");
        show(web);
        WebSettings s = web.getSettings();
        s.setJavaScriptEnabled(true);
        s.setDomStorageEnabled(true);
        s.setSupportMultipleWindows(false);
        Uri home = Uri.parse(ControlPlane.BASE_URL);
        web.setWebChromeClient(new WebChromeClient());
        web.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest req) {
                Uri u = req.getUrl();
                if (home.getScheme().equals(u.getScheme()) && home.getHost().equals(u.getHost())
                        && home.getPort() == u.getPort()) {
                    return false;
                }
                // Links out of the control plane (a PR, a docs page) open in the browser.
                try {
                    startActivity(new Intent(Intent.ACTION_VIEW, u));
                } catch (ActivityNotFoundException ignored) {
                    // Nothing on the phone opens it.
                }
                return true;
            }

            @Override
            public void doUpdateVisitedHistory(WebView view, String url, boolean reload) {
                updateBack();
            }
        });
        web.setDownloadListener((url, userAgent, disposition, mime, length) -> download(url, disposition, mime));
        web.loadUrl(ControlPlane.BASE_URL + "/");
    }

    /** A file from the Files tab goes to Downloads, with this run's session cookie. */
    private void download(String url, String disposition, String mime) {
        String name = URLUtil.guessFileName(url, disposition, mime);
        DownloadManager.Request req = new DownloadManager.Request(Uri.parse(url))
                .addRequestHeader("Cookie", ControlPlane.COOKIE + "=" + ControlPlane.get().secret)
                .setMimeType(mime)
                .setTitle(name)
                .setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED)
                .setDestinationInExternalPublicDir(Environment.DIRECTORY_DOWNLOADS, name);
        getSystemService(DownloadManager.class).enqueue(req);
        Toast.makeText(this, getString(R.string.downloading, name), Toast.LENGTH_SHORT).show();
    }

    private void updateBack() {
        boolean want = web != null && web.canGoBack();
        if (want == backRegistered) {
            return;
        }
        OnBackInvokedDispatcher d = getOnBackInvokedDispatcher();
        if (want) {
            d.registerOnBackInvokedCallback(OnBackInvokedDispatcher.PRIORITY_DEFAULT, back);
        } else {
            d.unregisterOnBackInvokedCallback(back);
        }
        backRegistered = want;
    }

    private void destroyWeb() {
        if (web != null) {
            web.destroy();
            web = null;
        }
        updateBack();
    }

    private LinearLayout column() {
        LinearLayout box = new LinearLayout(this);
        box.setOrientation(LinearLayout.VERTICAL);
        box.setPadding(dp(24), dp(24), dp(24), dp(24));
        return box;
    }

    private View scroll(View content) {
        android.widget.ScrollView s = new android.widget.ScrollView(this);
        s.addView(content);
        return s;
    }

    private static LinearLayout.LayoutParams wrap() {
        LinearLayout.LayoutParams p = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT,
                ViewGroup.LayoutParams.WRAP_CONTENT);
        p.topMargin = 24;
        return p;
    }

    private int dp(int v) {
        return Math.round(v * getResources().getDisplayMetrics().density);
    }

    /** What the UI can ask of the app, as window.orchestrateApp (imports.js). */
    private final class Host {
        @JavascriptInterface
        public void keys() {
            main.post(MainActivity.this::showKeysForm);
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        // Stop in the notification may have ended it while the app was away.
        if (web != null) {
            main.removeCallbacks(watch);
            main.post(watch);
        }
    }

    @Override
    protected void onPause() {
        super.onPause();
        CookieManager.getInstance().flush();
    }

    @Override
    protected void onDestroy() {
        main.removeCallbacks(watch);
        destroyWeb();
        // Leaving the app ends the control plane, as closing the desktop window does.
        if (isFinishing()) {
            ControlPlane.get().stop();
            ControlPlaneService.stop(this);
        }
        super.onDestroy();
    }
}
