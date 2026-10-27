plugins {
    id("com.android.application")
    id("com.chaquo.python")
}

// CI passes the run number, so each rolling release installs over the last.
val buildNumber = (findProperty("buildNumber") as String?)?.toInt() ?: 1

val orchestrator = rootDir.resolve("../orchestrator")
val uiDist = orchestrator.resolve("ui/dist")
val gitLibs = rootDir.resolve("git/out")
// Sync's Automerge, built for the phone (automerge/build.sh): PyPI has no Android wheel.
val automergeWheel = rootDir.resolve("automerge/out/automerge-0.2.0.dev4-cp312-cp312-android_33_arm64_v8a.whl")

// The control package next to the built ultra UI, as control/app.py expects
// to find it (UI_DIR = ../ui/dist).
val controlPlane = tasks.register<Sync>("controlPlane") {
    doFirst {
        check(uiDist.resolve("app.wasm").exists()) {
            "build the UI first: cd orchestrator && pixi run build-ui"
        }
        check(gitLibs.resolve("arm64-v8a/libgit.so").exists()) {
            "build git first: ANDROID_NDK_HOME=... android/git/build.sh android/git/out/arm64-v8a"
        }
        check(automergeWheel.exists()) {
            "build automerge first: ANDROID_NDK_HOME=... android/automerge/build.sh android/automerge/out"
        }
    }
    from(orchestrator.resolve("control")) {
        into("control")
        exclude("__pycache__")
    }
    from(uiDist) { into("ui/dist") }
    into(layout.buildDirectory.dir("generated/python"))
}

android {
    namespace = "dev.orchestrate.app"
    compileSdk = 36

    defaultConfig {
        applicationId = "dev.orchestrate.app"
        minSdk = 33
        targetSdk = 36
        versionCode = buildNumber
        versionName = "0.1.$buildNumber"
        ndk {
            abiFilters += listOf("arm64-v8a")
        }
    }

    sourceSets.getByName("main") {
        jniLibs.srcDir(gitLibs)
    }

    // The release key comes from the environment (README.md): Android
    // installs an update only when it is signed with the same key as the app
    // already on the phone.
    signingConfigs {
        create("release") {
            storeFile = System.getenv("ANDROID_KEYSTORE")?.let { file(it) }
            storePassword = System.getenv("ANDROID_KEYSTORE_PASSWORD")
            keyAlias = System.getenv("ANDROID_KEY_ALIAS")
            keyPassword = System.getenv("ANDROID_KEY_PASSWORD")
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("release")
        }
    }

    packaging {
        // git runs from the native library directory, so it is installed as files.
        jniLibs.useLegacyPackaging = true
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

chaquopy {
    defaultConfig {
        version = "3.12"
        pip {
            install("-r", "requirements.txt")
            install(automergeWheel.path)
        }
    }
    sourceSets.getByName("main") {
        srcDir(layout.buildDirectory.dir("generated/python").get().asFile)
    }
}

tasks.matching {
    (it.name.startsWith("merge") && it.name.endsWith("PythonSources")) ||
        (it.name.startsWith("generate") && it.name.endsWith("PythonRequirements"))
}.configureEach {
    dependsOn(controlPlane)
}
