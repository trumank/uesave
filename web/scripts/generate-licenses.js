#!/usr/bin/env node
import { execSync } from "child_process";
import { writeFileSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const webDir = join(__dirname, "..");
const rootDir = join(webDir, "..");

// Get Rust licenses via cargo-about
function getRustLicenses() {
  try {
    const output = execSync(
      `cargo about generate --manifest-path uesave_wasm/Cargo.toml --format json`,
      { cwd: rootDir, encoding: "utf-8", maxBuffer: 10 * 1024 * 1024 }
    );
    const data = JSON.parse(output);

    // Transform to simpler format: group by license, list packages
    const byLicense = {};
    for (const license of data.licenses) {
      const id = license.id;
      if (!byLicense[id]) {
        byLicense[id] = {
          name: license.name,
          text: license.text,
          packages: [],
        };
      }
      for (const used of license.used_by) {
        byLicense[id].packages.push({
          name: used.crate.name,
          version: used.crate.version,
          repository: used.crate.repository,
        });
      }
    }

    return Object.entries(byLicense).map(([id, data]) => ({
      id,
      ...data,
    }));
  } catch (e) {
    console.error("Failed to get Rust licenses:", e.message);
    return [];
  }
}

// Get JS licenses via license-checker
function getJsLicenses() {
  try {
    const output = execSync(`npx license-checker --json --production`, {
      cwd: webDir,
      encoding: "utf-8",
      maxBuffer: 10 * 1024 * 1024,
    });
    const data = JSON.parse(output);

    // Transform to simpler format: group by license
    const byLicense = {};
    for (const [pkgName, info] of Object.entries(data)) {
      // Skip the project itself
      if (info.path === webDir) continue;
      const license = info.licenses || "Unknown";
      if (!byLicense[license]) {
        byLicense[license] = {
          name: license,
          packages: [],
        };
      }

      // Read license text if available
      let licenseText = null;
      if (info.licenseFile) {
        try {
          licenseText = readFileSync(info.licenseFile, "utf-8");
        } catch {}
      }

      const [name, version] = pkgName.split("@").filter(Boolean);
      byLicense[license].packages.push({
        name: pkgName.startsWith("@") ? "@" + name : name,
        version: pkgName.startsWith("@") ? version : pkgName.split("@")[1],
        repository: info.repository,
        licenseText,
      });
    }

    // For each license group, use the first available license text
    return Object.entries(byLicense).map(([id, data]) => {
      const text =
        data.packages.find((p) => p.licenseText)?.licenseText || null;
      return {
        id,
        name: data.name,
        text,
        packages: data.packages.map(({ name, version, repository }) => ({
          name,
          version,
          repository,
        })),
      };
    });
  } catch (e) {
    console.error("Failed to get JS licenses:", e.message);
    return [];
  }
}

const rustLicenses = getRustLicenses();
const jsLicenses = getJsLicenses();

const combined = {
  rust: rustLicenses,
  js: jsLicenses,
};

const outPath = join(webDir, "src", "lib", "licenses.json");
writeFileSync(outPath, JSON.stringify(combined, null, 2));
console.log(`Generated ${outPath}`);
console.log(
  `  Rust: ${rustLicenses.length} licenses, ${rustLicenses.reduce((n, l) => n + l.packages.length, 0)} packages`
);
console.log(
  `  JS: ${jsLicenses.length} licenses, ${jsLicenses.reduce((n, l) => n + l.packages.length, 0)} packages`
);
