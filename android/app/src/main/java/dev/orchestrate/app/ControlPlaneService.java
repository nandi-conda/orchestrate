package dev.orchestrate.app;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.os.IBinder;
import android.os.PowerManager;

/**
 * Keeps the app's process running while the control plane runs. Without it
 * Android freezes the app as soon as its window leaves the screen, the
 * control plane with it: a worker's first attach installs the runner on a
 * sprite for minutes, and a thread's turn runs for as long as it takes, all
 * over connections the control plane holds open. A foreground service (with
 * its notification) is what keeps an app's process from being frozen, and a
 * partial wake lock keeps the CPU on with the screen off.
 *
 * It runs from the control plane's start until it stops: leaving the app
 * (Back out of it, or swipe it away), or Stop in the notification.
 */
public final class ControlPlaneService extends Service {
    private static final String CHANNEL = "control-plane";
    private static final int NOTIFICATION = 1;
    private static final String STOP = "dev.orchestrate.app.STOP";

    private PowerManager.WakeLock wake;

    static void start(Context context) {
        context.startForegroundService(new Intent(context, ControlPlaneService.class));
    }

    static void stop(Context context) {
        context.stopService(new Intent(context, ControlPlaneService.class));
    }

    @Override
    public void onCreate() {
        super.onCreate();
        NotificationManager nm = getSystemService(NotificationManager.class);
        nm.createNotificationChannel(new NotificationChannel(CHANNEL, getString(R.string.channel_name),
                NotificationManager.IMPORTANCE_LOW));
        wake = getSystemService(PowerManager.class)
                .newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "orchestrate:control-plane");
        wake.acquire();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && STOP.equals(intent.getAction())) {
            ControlPlane.get().stop();
            stopSelf();
            return START_NOT_STICKY;
        }
        startForeground(NOTIFICATION, notification(), ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE);
        return START_NOT_STICKY;
    }

    private Notification notification() {
        PendingIntent open = PendingIntent.getActivity(this, 0,
                new Intent(this, MainActivity.class).setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),
                PendingIntent.FLAG_IMMUTABLE);
        PendingIntent stop = PendingIntent.getService(this, 1,
                new Intent(this, ControlPlaneService.class).setAction(STOP), PendingIntent.FLAG_IMMUTABLE);
        return new Notification.Builder(this, CHANNEL)
                .setSmallIcon(R.drawable.ic_launcher_foreground)
                .setContentTitle(getString(R.string.app_name))
                .setContentText(getString(R.string.running))
                .setContentIntent(open)
                .setOngoing(true)
                .addAction(new Notification.Action.Builder(null, getString(R.string.stop), stop).build())
                .build();
    }

    @Override
    public void onDestroy() {
        if (wake.isHeld()) {
            wake.release();
        }
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }
}
