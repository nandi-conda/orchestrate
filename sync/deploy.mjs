// Deploy the sync server: `pixi run deploy-sync` at the repository root (or
// `pixi run deploy` here). Safe to run again.
//
// 1. Signs in to Cloudflare with `wrangler login` (the browser) unless wrangler
//    already is.
// 2. Makes the R2 bucket unless it exists, and deploys the Worker.
// 3. When the Worker's URL is not the control plane's DEFAULT_URL (another
//    Cloudflare account), keeps it as ORCHESTRATE_SYNC_URL in this computer's
//    keyring, where the control plane reads it.
//
// The relay is public: it keeps no secret, and each space is reached by its key.
import { spawnSync } from "node:child_process";

const BUCKET = "orchestrate-sync";
const DEFAULT_URL = "https://orchestrate-sync.nandithebull.workers.dev"; // control/sync.py's

function run(cmd, args, { input, interactive = false, check = true } = {}) {
  const r = spawnSync(cmd, args, {
    encoding: "utf8", input, stdio: interactive ? "inherit" : ["pipe", "pipe", "pipe"],
  });
  if (check && r.status !== 0) {
    console.error(`${cmd} ${args.join(" ")} failed${interactive ? "" : `:\n${r.stdout}${r.stderr}`}`);
    process.exit(1);
  }
  return r;
}

const wrangler = (args) => run("npx", ["wrangler", ...args]).stdout;

if (run("npx", ["wrangler", "whoami", "--json"], { check: false }).status !== 0) {
  console.log("Signing in to Cloudflare (a browser window opens)...");
  run("npx", ["wrangler", "login"], { interactive: true });
}

if (!wrangler(["r2", "bucket", "list"]).includes(BUCKET)) {
  wrangler(["r2", "bucket", "create", BUCKET]);
  console.log(`Made the R2 bucket ${BUCKET}.`);
}

const deployed = wrangler(["deploy"]);
const url = (deployed.match(/https:\/\/[^\s]+\.workers\.dev/) || [])[0];
if (!url) {
  console.error(`wrangler deploy printed no workers.dev URL:\n${deployed}`);
  process.exit(1);
}
console.log(`Deployed the sync server at ${url}.`);

if (url !== DEFAULT_URL) {
  // Through Python's keyring, the library the control plane reads it with.
  run("python", ["-c", "import sys, keyring; keyring.set_password('orchestrate', 'ORCHESTRATE_SYNC_URL', sys.stdin.read())"],
    { input: url });
  console.log(`It is not the default server, so this computer's keyring has it as ORCHESTRATE_SYNC_URL;
set the same on your other computers (Settings, or pixi run set-key ORCHESTRATE_SYNC_URL).`);
}
