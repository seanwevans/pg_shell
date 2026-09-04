// Assembles the GitHub Pages site.
//
// Nothing in web/ duplicates repository content: the SQL the page installs and
// the htmx build it loads are copied straight out of sql/ and html/vendor/ at
// build time, so the published demo can never drift from the source it claims
// to be running.

import { cp, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "..");
const out = resolve(repo, process.argv[2] ?? "_site");
const require = createRequire(import.meta.url);

// PGlite ships its WebAssembly build alongside the JS; source maps, the CommonJS
// entry points and the optional extension bundles are not fetched by the page.
const PGLITE_SKIP = [/\.map$/, /\.cjs$/, /\.tar\.gz$/, /^contrib$/];

async function copyPglite(destination) {
  // package.json is not exported, so resolve the CJS entry point and take its
  // directory: node_modules/@electric-sql/pglite/dist.
  const dist = dirname(require.resolve("@electric-sql/pglite"));
  await mkdir(destination, { recursive: true });
  let bytes = 0;
  for (const entry of await readdir(dist)) {
    if (PGLITE_SKIP.some((pattern) => pattern.test(entry))) continue;
    await cp(join(dist, entry), join(destination, entry), { recursive: true });
    const info = await stat(join(dist, entry));
    if (info.isFile()) bytes += info.size;
  }
  return bytes;
}

async function main() {
  await rm(out, { recursive: true, force: true });
  await mkdir(out, { recursive: true });

  await cp(join(here, "index.html"), join(out, "index.html"));
  await cp(join(here, "assets"), join(out, "assets"), { recursive: true });

  // The page installs pg_shell from these files at runtime; they are the
  // repository's, not a copy maintained here.
  await cp(join(repo, "sql"), join(out, "sql"), { recursive: true });

  await mkdir(join(out, "vendor"), { recursive: true });
  await cp(
    join(repo, "html", "vendor", "htmx-1.9.12.min.js"),
    join(out, "vendor", "htmx-1.9.12.min.js")
  );
  const pgliteBytes = await copyPglite(join(out, "vendor", "pglite"));

  // GitHub Pages runs Jekyll over the artifact unless told not to, which would
  // drop the underscore-prefixed files PGlite ships.
  await writeFile(join(out, ".nojekyll"), "");

  const manifest = join(dirname(require.resolve("@electric-sql/pglite")), "..", "package.json");
  const { version } = JSON.parse(await readFile(manifest, "utf8"));
  console.log(`built ${out}`);
  console.log(`  pglite ${version} (${(pgliteBytes / 1024 / 1024).toFixed(1)} MB)`);
  console.log(`  sql/   ${(await readdir(join(out, "sql"))).length} files`);
}

await main();
