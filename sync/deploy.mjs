// Deploy the sync server to your Cloudflare account: `pixi run deploy`.
//
// Needs CLOUDFLARE_API_TOKEN (Account > Workers Scripts: Edit and Account >
// Workers R2 Storage: Edit) and CLOUDFLARE_ACCOUNT_ID. Makes the R2 bucket the
// first time, deploys the Worker, and gives it a SYNC_TOKEN secret the first
// time (or with --new-token, which every computer then needs). Prints the URL
// and, when it made one, the token: set them as ORCHESTRATE_SYNC_URL and
// ORCHESTRATE_SYNC_TOKEN on every computer that shares projects.
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";

const BUCKET = "orchestrate-sync";

const missing = ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID"].filter((k) => !process.env[k]);
if (missing.length) {
  console.error(`Set ${missing.join(" and ")} first:
  CLOUDFLARE_API_TOKEN   an API token with Account > Workers Scripts: Edit and Account > Workers R2 Storage: Edit
                         (the "Edit Cloudflare Workers" template has both)
  CLOUDFLARE_ACCOUNT_ID  your account id, from the dashboard`);
  process.exit(1);
}

function wrangler(args, input) {
  const r = spawnSync("npx", ["wrangler", ...args], { encoding: "utf8", input, stdio: ["pipe", "pipe", "pipe"] });
  if (r.status !== 0) {
    console.error(`wrangler ${args.join(" ")} failed:\n${r.stdout}${r.stderr}`);
    process.exit(1);
  }
  return r.stdout;
}

if (!wrangler(["r2", "bucket", "list"]).includes(BUCKET)) {
  wrangler(["r2", "bucket", "create", BUCKET]);
  console.log(`made the R2 bucket ${BUCKET}`);
}
const deployed = wrangler(["deploy"]);
const url = (deployed.match(/https:\/\/[^\s]+\.workers\.dev/) || [])[0];
const secrets = JSON.parse(wrangler(["secret", "list", "--format", "json"]) || "[]");
let token = null;
if (!secrets.some((s) => s.name === "SYNC_TOKEN") || process.argv.includes("--new-token")) {
  token = randomBytes(32).toString("base64url");
  wrangler(["secret", "put", "SYNC_TOKEN"], token);
}
console.log(`\nThe sync server is deployed.\n\n  ORCHESTRATE_SYNC_URL=${url || "(see wrangler's output above)"}`);
if (token) {
  console.log(`  ORCHESTRATE_SYNC_TOKEN=${token}\n\nKeep the token: it is shown this once. Set both on every computer that ` +
    "shares projects (environment, keyring with `pixi run set-key`, or the Android app's Keys screen).");
} else {
  console.log("\nThe Worker keeps the token it had; run with --new-token to make a new one.");
}
