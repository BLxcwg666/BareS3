import { cp, mkdir, rm, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, '..');
const sourceDir = path.join(rootDir, 'apps', 'bares3-frontend', 'dist');
const targetDir = path.join(rootDir, 'apps', 'bares3-server', 'internal', 'webui', 'dist');

try {
  await stat(sourceDir);
} catch {
  throw new Error(`Frontend build output not found at ${sourceDir}. Run \`pnpm build:frontend\` first.`);
}

await rm(targetDir, { recursive: true, force: true });
await mkdir(path.dirname(targetDir), { recursive: true });
await cp(sourceDir, targetDir, { recursive: true });

console.log(`Synced web UI assets to ${path.relative(rootDir, targetDir)}`);
