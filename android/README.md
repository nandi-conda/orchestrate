# Android app

The orchestrator on a phone: the ultra UI (`orchestrator/ui`) in a WebView,
with the control plane (`orchestrator/control`) running in the app's process,
as the desktop window runs it. Using it: [docs/guide/android.md](../docs/guide/android.md).

| Path | What |
| --- | --- |
| `app/src/main/java/.../MainActivity.java` | the window: Keys screen, then the WebView |
| `app/src/main/java/.../ControlPlane.java` | starts and stops the control plane on its own thread |
| `app/src/main/java/.../ControlPlaneService.java` | foreground service (notification + wake lock) so Android does not freeze the control plane off screen |
| `app/src/main/java/.../Keys.java` | the keys, encrypted with an Android Keystore key |
| `app/src/main/python/orchestrate_android.py` | the control plane's entry point on the phone: git on PATH, the session cookie, sprites |
| `app/requirements.txt` | the control plane's Python packages, pinned as `orchestrator/pixi.lock` has them |
| `git/build.sh` | cross-compiles git (with zlib, OpenSSL, curl) for arm64 Android |

The control plane is not copied: the build takes `orchestrator/control` and
the built `orchestrator/ui/dist` as they are (the `controlPlane` task in
`app/build.gradle.kts`).

## Build

You need JDK 17+, the Android SDK (`ANDROID_HOME`, or `sdk.dir` in
`local.properties`), the Android NDK for git, and pixi.

```sh
cd orchestrator && pixi run build-ui && cd ..
ANDROID_NDK_HOME=~/Android/Sdk/ndk/<version> android/git/build.sh android/git/out/arm64-v8a
cd android
PATH="$PWD/../orchestrator/.pixi/envs/default/bin:$PATH" ./gradlew assembleRelease   # Chaquopy needs python3.12 on PATH
```

The APK is `app/build/outputs/apk/release/app-release.apk`. A release build
is signed with the key the environment names:

| Variable | What |
| --- | --- |
| `ANDROID_KEYSTORE` | path to the `.jks` keystore |
| `ANDROID_KEYSTORE_PASSWORD` | the keystore's password |
| `ANDROID_KEY_ALIAS` | the key's alias in it |
| `ANDROID_KEY_PASSWORD` | the key's password |

## Rolling release

`.github/workflows/android.yml` builds the app on each push to `main` that
touches `android/`, `orchestrator/control/` or `orchestrator/ui/`, moves the
`rolling` tag to that commit and uploads the APK to its release as
`orchestrator.apk`:
<https://github.com/nandi-conda/orchestrate/releases/download/rolling/orchestrator.apk>.
The build number is the workflow's run number, so each build installs over
the last.

### Release key

Android installs an update only over an app signed with the same key, so CI
signs every build with one key, from four repository secrets:

| Secret | Format |
| --- | --- |
| `ANDROID_KEYSTORE_BASE64` | the `.jks` file, base64 on one line |
| `ANDROID_KEYSTORE_PASSWORD` | text |
| `ANDROID_KEY_ALIAS` | text |
| `ANDROID_KEY_PASSWORD` | text |

Make them once, on your computer (needs a JDK's `keytool` and `gh` signed in):

```sh
android/release-key.sh            # keystore at ~/.config/orchestrate/android-release.jks
```

It makes the keystore with a random password, kept beside it in
`android-release.jks.password` (both readable by you only), and sets the four
secrets with `gh secret set`. Run again with the same path, it sets the
secrets from the existing key.

Keep the keystore and its password file safe: with another key, phones have to uninstall the
app (and lose its data) before the next build installs.
