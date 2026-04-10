import type { BucketAccessAction, BucketAccessMode, S3CredentialPermission } from './api';
import type { SizeUnit } from './types';

export const themeStorageKey = 'bares3-theme-mode';

export const sizeUnitOptions: Array<{ label: SizeUnit; value: SizeUnit; bytes: number }> = [
  { label: 'MB', value: 'MB', bytes: 1024 ** 2 },
  { label: 'GB', value: 'GB', bytes: 1024 ** 3 },
  { label: 'TB', value: 'TB', bytes: 1024 ** 4 },
];

export const bucketAccessModeOptions: Array<{ label: string; value: BucketAccessMode }> = [
  { label: 'Private', value: 'private' },
  { label: 'Public', value: 'public' },
  { label: 'Custom', value: 'custom' },
];

export const bucketAccessActionOptions: Array<{ label: string; value: BucketAccessAction }> = [
  { label: 'Public', value: 'public' },
  { label: 'Require auth', value: 'authenticated' },
  { label: 'Deny', value: 'deny' },
];

export const s3CredentialPermissionOptions: Array<{ label: string; value: S3CredentialPermission }> = [
  { label: 'Read & write', value: 'read_write' },
  { label: 'Read only', value: 'read_only' },
];

export const pageMeta: Record<string, { title: string; note: string }> = {
  '/overview': { title: 'Overview', note: 'Buckets, routes, and current disk state.' },
  '/buckets': { title: 'Buckets', note: 'Readable roots with clear exposure rules.' },
  '/buckets/access': { title: 'Bucket Access', note: 'Ordered rules for public, authenticated, and denied paths.' },
  '/access-keys': { title: 'Access Keys', note: 'Managed S3 credentials with bucket scope and usage tracking.' },
  '/browser': { title: 'Browser', note: 'Objects, metadata, and current path context.' },
  '/audit': { title: 'Audit Logs', note: 'Recent console actions and storage changes.' },
  '/links': { title: 'Share links', note: 'Revocable token links for previews and downloads.' },
  '/domains': { title: 'Domains', note: 'Map public hostnames to bucket prefixes for static delivery.' },
  '/settings': { title: 'Settings', note: 'Defaults that make the storage layer predictable.' },
  '/sync': { title: 'Replication', note: 'Remote links, pull tokens, and replication health.' },
};
