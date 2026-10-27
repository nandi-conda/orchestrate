package dev.orchestrate.app;

import android.content.Context;
import android.content.SharedPreferences;
import android.security.keystore.KeyGenParameterSpec;
import android.security.keystore.KeyProperties;
import android.util.Base64;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/**
 * The control plane's keys, as the Keys screen stores them: each encrypted
 * with an AES key that the Android Keystore holds and never hands out, then
 * kept in the app's private preferences. The control plane gets them as
 * environment variables, where control/secrets.py looks first.
 */
final class Keys {
    static final class Key {
        final String name;
        final boolean required;
        final int help;

        Key(String name, boolean required, int help) {
            this.name = name;
            this.required = required;
            this.help = help;
        }
    }

    static final Key[] ALL = {
        new Key("DEEPSEEK_API_KEY", true, R.string.key_deepseek),
        new Key("SPRITES_TOKEN", true, R.string.key_sprites),
        new Key("OPENGATEWAY_API_TOKEN", false, R.string.key_opengateway),
        new Key("GITHUB_TOKEN", false, R.string.key_github),
        new Key("ORCHESTRATE_SYNC_URL", false, R.string.key_sync_url),
        new Key("ORCHESTRATE_SYNC_TOKEN", false, R.string.key_sync_token),
    };

    private static final String KEYSTORE = "AndroidKeyStore";
    private static final String ALIAS = "orchestrate-keys";
    private static final String CIPHER = "AES/GCM/NoPadding";
    private static final int IV_BYTES = 12;

    private final SharedPreferences prefs;

    Keys(Context context) {
        prefs = context.getSharedPreferences("keys", Context.MODE_PRIVATE);
    }

    boolean complete() {
        for (Key k : ALL) {
            if (k.required && get(k.name).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    String get(String name) {
        String stored = prefs.getString(name, "");
        if (stored.isEmpty()) {
            return "";
        }
        try {
            ByteBuffer in = ByteBuffer.wrap(Base64.decode(stored, Base64.NO_WRAP));
            byte[] iv = new byte[IV_BYTES];
            in.get(iv);
            byte[] sealed = new byte[in.remaining()];
            in.get(sealed);
            Cipher c = Cipher.getInstance(CIPHER);
            c.init(Cipher.DECRYPT_MODE, secret(), new GCMParameterSpec(128, iv));
            return new String(c.doFinal(sealed), StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | java.io.IOException | IllegalArgumentException e) {
            throw new IllegalStateException("cannot read the stored " + name, e);
        }
    }

    void set(String name, String value) {
        if (value.isEmpty()) {
            prefs.edit().remove(name).apply();
            return;
        }
        try {
            Cipher c = Cipher.getInstance(CIPHER);
            c.init(Cipher.ENCRYPT_MODE, secret());
            byte[] sealed = c.doFinal(value.getBytes(StandardCharsets.UTF_8));
            ByteBuffer out = ByteBuffer.allocate(IV_BYTES + sealed.length).put(c.getIV()).put(sealed);
            prefs.edit().putString(name, Base64.encodeToString(out.array(), Base64.NO_WRAP)).apply();
        } catch (GeneralSecurityException | java.io.IOException e) {
            throw new IllegalStateException("cannot store " + name, e);
        }
    }

    private static SecretKey secret() throws GeneralSecurityException, java.io.IOException {
        KeyStore ks = KeyStore.getInstance(KEYSTORE);
        ks.load(null);
        if (ks.containsAlias(ALIAS)) {
            return (SecretKey) ks.getKey(ALIAS, null);
        }
        KeyGenerator g = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, KEYSTORE);
        g.init(new KeyGenParameterSpec.Builder(ALIAS, KeyProperties.PURPOSE_ENCRYPT | KeyProperties.PURPOSE_DECRYPT)
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .build());
        return g.generateKey();
    }
}
