#!/usr/bin/env bash
# Cross-compile git for the app: the control plane's hub runs git (worktrees,
# rebases, bundles, pushes over https), and Android has none.
#
#   ANDROID_NDK_HOME=/path/to/ndk android/git/build.sh <out dir>
#
# Writes <out>/libgit.so and <out>/libgit-remote-http.so, two arm64 programs
# named as libraries: Android runs programs only from the app's native library
# directory, and installs only lib*.so files there. zlib, OpenSSL and curl are
# linked in statically; the C library is the phone's own. The app links git's
# command names (git, git-remote-https, ...) to them at startup (git_android.py).
set -euo pipefail

API=33
TARGET=aarch64-linux-android$API
ZLIB=1.3.2 OPENSSL=3.5.9 CURL=8.23.0 GIT=2.56.0
SOURCES=(
  "https://github.com/madler/zlib/releases/download/v$ZLIB/zlib-$ZLIB.tar.gz bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16"
  "https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL/openssl-$OPENSSL.tar.gz 603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a"
  "https://curl.se/download/curl-$CURL.tar.xz 8dadd7d2536a314f982ad05a651b713f426bf7d02cc2c76cffb9796f2218e6c5"
  "https://mirrors.edge.kernel.org/pub/software/scm/git/git-$GIT.tar.xz 26c56c296b38c0695b26fa95f475f1d01704d2d38e73465ca30b0b2f5dc789d3"
)

OUT=$(realpath -m "${1:?usage: build.sh <out dir>}")
HERE=$(cd "$(dirname "$0")" && pwd)
: "${ANDROID_NDK_HOME:?set ANDROID_NDK_HOME to the NDK}"
TOOLS=$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/linux-x86_64/bin
export PATH=$TOOLS:$PATH
export CC=$TARGET-clang AR=llvm-ar RANLIB=llvm-ranlib STRIP=llvm-strip
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
PREFIX=$WORK/prefix
JOBS=$(nproc)

cd "$WORK"
for s in "${SOURCES[@]}"; do
  read -r url sum <<<"$s"
  curl -sSfL --retry 4 -o "$(basename "$url")" "$url"
  echo "$sum  $(basename "$url")" | sha256sum -c --quiet
  tar xf "$(basename "$url")"
done

(cd zlib-$ZLIB && ./configure --static --prefix="$PREFIX" >/dev/null && make -j"$JOBS" install >/dev/null)

(cd openssl-$OPENSSL &&
  ANDROID_NDK_ROOT=$ANDROID_NDK_HOME ./Configure android-arm64 -D__ANDROID_API__=$API no-shared no-tests \
    no-docs no-apps --prefix="$PREFIX" --libdir=lib >/dev/null &&
  make -j"$JOBS" >/dev/null && make install_sw >/dev/null)

# The CA bundle is the app's (certifi), given to git as http.sslCAInfo.
(cd curl-$CURL &&
  ./configure --host=aarch64-linux-android --prefix="$PREFIX" --disable-shared --enable-static \
    --with-openssl="$PREFIX" --with-zlib="$PREFIX" --without-libpsl --without-libidn2 --without-nghttp2 \
    --without-brotli --without-zstd --without-ca-bundle --without-ca-path --disable-ldap --disable-docs \
    --disable-manual >/dev/null &&
  make -j"$JOBS" install >/dev/null)

# git's shell stays /bin/sh, which Android has had since 10 (a link to
# /system/bin). No Rust, gettext, perl, python, Tcl or expat (push over WebDAV):
# the control plane needs none of them.
cd git-$GIT
make -j"$JOBS" git git-remote-http \
  uname_S=Linux uname_M=aarch64 \
  CC="$CC" AR="$AR" CFLAGS="-O2 -include $HERE/android-compat.h" \
  prefix=/nonexistent \
  NO_RUST=1 NO_GETTEXT=1 NO_TCLTK=1 NO_PERL=1 NO_PYTHON=1 NO_EXPAT=1 NO_ICONV=1 NO_REGEX=1 NO_INSTALL_HARDLINKS=1 \
  NO_OPENSSL=1 CURLDIR="$PREFIX" CURL_LDFLAGS="-L$PREFIX/lib -lcurl -lssl -lcrypto -lz" \
  ZLIB_PATH="$PREFIX" HAVE_DEV_TTY=1 HAVE_SYNC_FILE_RANGE= PTHREAD_LIBS= >/dev/null
mkdir -p "$OUT"
"$STRIP" -o "$OUT/libgit.so" git
"$STRIP" -o "$OUT/libgit-remote-http.so" git-remote-http
ls -l "$OUT"
