import { spawnSync } from "node:child_process";
import { mkdirSync, rmSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, "..", "..");
const crateDir = path.join(repoRoot, "crates", "ferrokinesis-wasm");
const outDir = path.join(repoRoot, "demo", "src", "generated");
const relativeOutDir = path.relative(crateDir, outDir);
const useWasmOpt = process.env.FERROKINESIS_WASM_OPT === "1";

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

const args = [
  "build",
  ".",
  "--target",
  "web",
  "--release",
  "--out-dir",
  relativeOutDir,
  "--out-name",
  "ferrokinesis_wasm",
];

if (!useWasmOpt) {
  args.push("--no-opt");
}

console.log(
  `Building browser WASM package (${useWasmOpt ? "with wasm-opt" : "without wasm-opt"})...`,
);

const result = spawnSync("wasm-pack", args, {
  cwd: crateDir,
  stdio: "inherit",
});

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

const wasmPath = path.join(outDir, "ferrokinesis_wasm_bg.wasm");
const jsPath = path.join(outDir, "ferrokinesis_wasm.js");

console.log(`WASM size: ${formatBytes(statSync(wasmPath).size)}`);
console.log(`JS glue size: ${formatBytes(statSync(jsPath).size)}`);

function formatBytes(size) {
  const kib = size / 1024;
  const mib = kib / 1024;
  return `${size} bytes (${kib.toFixed(1)} KiB / ${mib.toFixed(2)} MiB)`;
}
