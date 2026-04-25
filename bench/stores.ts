/**
 * Scratch file stores.
 *
 * Every file-store measurement wants a directory nothing else has written to, because the
 * cost of an append depends on what is already in the store and the cost of a recovery
 * depends on what is already on disk. Sharing one directory between benchmarks would make
 * each one's numbers depend on the order they happened to run in.
 *
 * The directory is returned alongside the store rather than read back off it. `FileEventStore`
 * keeps its root private, which is correct — nothing in the control plane has any business
 * knowing where the log lives — so the caller that chose the path is the one that remembers
 * it.
 */

import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { FileEventStore } from '../src/events/file-store.js';

export interface ScratchStore {
  readonly store: FileEventStore;
  readonly dir: string;
}

const created: string[] = [];

/** A root for the whole benchmark run, so one `rm` cleans up everything. */
export async function scratchRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'parliament-bench-'));
  created.push(root);
  return root;
}

export async function openScratchStore(root: string, label = 'store'): Promise<ScratchStore> {
  const dir = await mkdtemp(join(root, `${label}-`));
  return { store: await FileEventStore.open(dir), dir };
}

export async function cleanUp(): Promise<void> {
  await Promise.all(created.map((dir) => rm(dir, { recursive: true, force: true })));
  created.length = 0;
}
