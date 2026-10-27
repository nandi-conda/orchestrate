#!/usr/bin/env bash
# Cross-compile Automerge's Python package for the app: the control plane's
# sync (control/sync.py) needs it, and PyPI has no Android wheel of it.
#
#   ANDROID_NDK_HOME=/path/to/ndk android/automerge/build.sh <out dir>
#
# Writes <out>/automerge-<version>-cp312-cp312-android_33_arm64_v8a.whl, which
# app/build.gradle.kts gives Chaquopy's pip. The package is Rust (PyO3) built
# with maturin from its sdist, the version orchestrator/pixi.toml pins.
#
# An Android extension module links libpython, and its file name ends in the
# app's Python's EXT_SUFFIX: both come from the Python that Chaquopy puts in
# the app (com.chaquo.python:target, the version Chaquopy 17.1 uses for 3.12),
# given to PyO3 and maturin as PYO3_CROSS_LIB_DIR.
set -euo pipefail

API=33
TARGET=aarch64-linux-android
AUTOMERGE=0.2.0.dev4 MATURIN=1.15.0 PYTHON_TARGET=3.12.15-0
MAVEN=https://repo1.maven.org/maven2/com/chaquo/python/target/$PYTHON_TARGET
SOURCES=(
  "https://files.pythonhosted.org/packages/source/a/automerge/automerge-$AUTOMERGE.tar.gz 68540722ecab8ecb22edc19d470b9a0718ac5b13ade17509f5ae793f2f8242d6"
  "$MAVEN/target-$PYTHON_TARGET-arm64-v8a.zip bcc3cb4edcff25c8c3103cf45ea35b66ef0a728bb2b3e7731d3c045093ff6d39"
  "$MAVEN/target-$PYTHON_TARGET-stdlib.zip 7cbf170fa5218fb86e8dcd4a8fbe440cf5904f70fc57c3c3748e8539f4d3b91a"
)

OUT=$(realpath -m "${1:?usage: build.sh <out dir>}")
: "${ANDROID_NDK_HOME:?set ANDROID_NDK_HOME to the NDK}"
TOOLS=$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/linux-x86_64/bin
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

cd "$WORK"
for s in "${SOURCES[@]}"; do
  read -r url sum <<<"$s"
  curl -sSfL --retry 4 -o "$(basename "$url")" "$url"
  echo "$sum  $(basename "$url")" | sha256sum -c --quiet
done
tar xf "automerge-$AUTOMERGE.tar.gz"

# The app's libpython3.12.so and its sysconfig data, side by side.
PYLIB=$WORK/python
unzip -q -j "target-$PYTHON_TARGET-arm64-v8a.zip" 'jniLibs/arm64-v8a/libpython3.12.so' -d "$PYLIB"
unzip -q "target-$PYTHON_TARGET-stdlib.zip" '_sysconfigdata__linux_.py' -d "$PYLIB"

python3 -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q "maturin==$MATURIN"
rustup target add "$TARGET"

cd "automerge-$AUTOMERGE"
CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER=$TOOLS/$TARGET$API-clang \
  CC_aarch64_linux_android=$TOOLS/$TARGET$API-clang AR_aarch64_linux_android=$TOOLS/llvm-ar \
  ANDROID_API_LEVEL=$API PYO3_CROSS_LIB_DIR=$PYLIB \
  "$WORK/venv/bin/maturin" build --release --locked --strip --target "$TARGET" -i python3.12 --out "$OUT"
ls -l "$OUT"
