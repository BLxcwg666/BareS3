import type { SizeUnit } from './types';

export const themeStorageKey = 'bares3-theme-mode';

export const sizeUnitOptions: Array<{ label: SizeUnit; value: SizeUnit; bytes: number }> = [
  { label: 'MB', value: 'MB', bytes: 1024 ** 2 },
  { label: 'GB', value: 'GB', bytes: 1024 ** 3 },
  { label: 'TB', value: 'TB', bytes: 1024 ** 4 },
];

export const pageMeta: Record<string, { title: string; note: string }> = {
  '/overview': { title: 'Overview', note: 'Buckets, routes, and current disk state.' },
  '/buckets': { title: 'Buckets', note: 'Readable roots with clear exposure rules.' },
  '/browser': { title: 'Browser', note: 'Objects, metadata, and current path context.' },
  '/audit': { title: 'Audit Logs', note: 'Recent console actions and storage changes.' },
  '/links': { title: 'Share links', note: 'Public routes and presigned downloads.' },
  '/settings': { title: 'Settings', note: 'Defaults that make the storage layer predictable.' },
};
