#!/usr/bin/env bash
# Make the Android release key and give it to CI: run once, on your computer.
#
#   android/release-key.sh [keystore path]
#
# Generates a keystore with a random password (unless the path already holds
# one this script made), then sets the four repository secrets the Android
# workflow signs with: ANDROID_KEYSTORE_BASE64, ANDROID_KEYSTORE_PASSWORD,
# ANDROID_KEY_ALIAS, ANDROID_KEY_PASSWORD. Needs keytool (any JDK) and gh,
# signed in with access to the repository's secrets.
#
# The keystore and its password stay next to each other, readable by you only:
# <keystore> and <keystore>.password. Keep both. Builds signed with another key
# install only after the app (and its data) is uninstalled from the phone.
# Running the script again with the same path sets the secrets from them again.
set -euo pipefail

REPO=nandi-conda/orchestrate
ALIAS=orchestrator
KEYSTORE=$(realpath -m "${1:-$HOME/.config/orchestrate/android-release.jks}")
PASSFILE=$KEYSTORE.password

for tool in keytool gh base64; do
  command -v "$tool" >/dev/null || { echo "release-key.sh: $tool is not installed" >&2; exit 1; }
done
gh auth status >/dev/null || { echo "release-key.sh: sign in first: gh auth login" >&2; exit 1; }

umask 077
if [ -e "$KEYSTORE" ]; then
  [ -r "$PASSFILE" ] || { echo "release-key.sh: $KEYSTORE exists without $PASSFILE; give another path" >&2; exit 1; }
  echo "Using the existing key in $KEYSTORE"
else
  mkdir -p "$(dirname "$KEYSTORE")"
  # PKCS12 keystores have one password for the store and the key.
  head -c 32 /dev/urandom | base64 | tr -d '/+=\n' > "$PASSFILE"
  keytool -genkeypair -keystore "$KEYSTORE" -storetype PKCS12 -alias "$ALIAS" -keyalg RSA -keysize 4096 \
    -validity 36500 -dname "CN=orchestrator" -storepass:file "$PASSFILE" -keypass:file "$PASSFILE"
  echo "Made $KEYSTORE (password in $PASSFILE)"
fi
PASSWORD=$(cat "$PASSFILE")

base64 < "$KEYSTORE" | tr -d '\n' | gh secret set ANDROID_KEYSTORE_BASE64 -R "$REPO"
gh secret set ANDROID_KEYSTORE_PASSWORD -R "$REPO" --body "$PASSWORD"
gh secret set ANDROID_KEY_ALIAS -R "$REPO" --body "$ALIAS"
gh secret set ANDROID_KEY_PASSWORD -R "$REPO" --body "$PASSWORD"
echo "Set the signing secrets on $REPO. Re-run the Android workflow: gh workflow run android.yml -R $REPO"
