export type MetricItem = {
  label: string;
  value: string;
  detail: string;
};

export type BucketRow = {
  key: string;
  purpose: string;
  root: string;
  mode: 'Public' | 'Private' | 'Signed only';
  size: string;
  objects: string;
  used: number;
  policy: string;
};

export type ActivityRow = {
  title: string;
  meta: string;
  time: string;
};

export type ObjectRow = {
  key: string;
  type: string;
  size: string;
  cache: string;
  updated: string;
  etag: string;
};

export type SettingGroup = {
  title: string;
  items: Array<{
    label: string;
    value: string;
  }>;
};

export const overviewMetrics: MetricItem[] = [
  { label: 'Buckets', value: '5', detail: '2 public / 2 private / 1 signed' },
  { label: 'Used', value: '61.4%', detail: '1.84 TB of mirrored capacity' },
  { label: 'Active links', value: '9', detail: 'public routes and signed downloads' },
];

export const bucketRows: BucketRow[] = [
  {
    key: 'gallery',
    purpose: 'Image host',
    root: '/data/gallery',
    mode: 'Public',
    size: '38.2 GB',
    objects: '1,248',
    used: 32,
    policy: 'Long cache, direct read',
  },
  {
    key: 'drops',
    purpose: 'Public downloads',
    root: '/data/drops',
    mode: 'Public',
    size: '412.7 GB',
    objects: '364',
    used: 61,
    policy: 'Alias route enabled',
  },
  {
    key: 'vault',
    purpose: 'Private archive',
    root: '/data/vault',
    mode: 'Private',
    size: '1.09 TB',
    objects: '8,114',
    used: 78,
    policy: 'Signed access only',
  },
  {
    key: 'patches',
    purpose: 'Release assets',
    root: '/data/patches',
    mode: 'Signed only',
    size: '86.4 GB',
    objects: '213',
    used: 45,
    policy: 'Short-lived share links',
  },
];

export const activityRows: ActivityRow[] = [
  {
    title: 'Renewed signed route for drops/win64/client.zip',
    meta: 'Expiry extended to 72 hours',
    time: '8 min ago',
  },
  {
    title: 'Uploaded gallery/2026/launch/mock-02.png',
    meta: 'Inherited cache-control from bucket defaults',
    time: '21 min ago',
  },
  {
    title: 'Finished metadata scan for vault',
    meta: 'No drift between files and sidecars',
    time: 'Today 07:14',
  },
  {
    title: 'Readonly key used on gallery',
    meta: '14.8k requests in the last 24 hours',
    time: 'Today 02:08',
  },
];

export const objectRows: ObjectRow[] = [
  {
    key: 'launch/mock-02.png',
    type: 'image/png',
    size: '2.4 MB',
    cache: 'public, max-age=31536000',
    updated: '2026-03-24 21:14',
    etag: '6f5c4aa3',
  },
  {
    key: 'launch/mock-02.meta.json',
    type: 'metadata',
    size: '812 B',
    cache: 'sidecar only',
    updated: '2026-03-24 21:14',
    etag: 'be12d71a',
  },
  {
    key: 'launch/mock-03.png',
    type: 'image/png',
    size: '2.1 MB',
    cache: 'public, max-age=31536000',
    updated: '2026-03-24 21:18',
    etag: '1170eb94',
  },
  {
    key: 'hero/banner-home.jpg',
    type: 'image/jpeg',
    size: '5.7 MB',
    cache: 'public, max-age=604800',
    updated: '2026-03-23 09:42',
    etag: '4d3acb31',
  },
  {
    key: 'notes/keep-original-filenames.txt',
    type: 'text/plain',
    size: '1.1 KB',
    cache: 'private',
    updated: '2026-03-22 11:08',
    etag: '9afcc012',
  },
];

export const nodeSummary = [
  { label: 'Console', value: 'BareS3 local-node' },
  { label: 'Endpoint', value: 'https://s3.bare.local' },
  { label: 'Region', value: 'home-lab-1' },
  { label: 'Write mode', value: 'temp file then atomic rename' },
];

export const consoleRules = [
  'Keep bucket names readable on disk.',
  'Expose public delivery explicitly, not by accident.',
  'Store metadata beside the real file, not in a hidden blob store.',
];

export const bucketTemplates = [
  'Image host - public reads, long cache, sidecar dimensions.',
  'Download shelf - stable aliases for versioned files.',
  'Private vault - strict defaults and signed delivery.',
];

export const browserInspector = [
  { label: 'Stored name', value: 'launch/mock-02.png' },
  { label: 'Content-Type', value: 'image/png' },
  { label: 'Cache-Control', value: 'public, max-age=31536000' },
  { label: 'ETag snapshot', value: '6f5c4aa3' },
  { label: 'Public route', value: '/pub/gallery/launch/mock-02.png' },
];

export const publishingNotes = [
  'Public routes fit image hosting and immutable assets.',
  'Alias routes keep download URLs stable while files rotate.',
  'Signed routes are the default for private delivery.',
];

export const settingGroups: SettingGroup[] = [
  {
    title: 'Endpoint identity',
    items: [
      { label: 'Console name', value: 'BareS3 local-node' },
      { label: 'S3 endpoint', value: 'https://s3.bare.local' },
      { label: 'Region label', value: 'home-lab-1' },
    ],
  },
  {
    title: 'Storage defaults',
    items: [
      { label: 'Bucket mapping', value: 'One bucket = one top-level folder' },
      { label: 'Metadata mode', value: 'Sidecar json with etag snapshot' },
      { label: 'Upload safety', value: 'Temp write then atomic rename' },
    ],
  },
  {
    title: 'Delivery rules',
    items: [
      { label: 'Range requests', value: 'Enabled' },
      { label: 'Public links', value: 'Per bucket policy' },
      { label: 'Default cache', value: 'Private unless published' },
    ],
  },
];

export const loginNotes = [
  { label: 'Buckets map to folders', value: 'Readable after shutdown' },
  { label: 'Metadata stays nearby', value: 'Sidecar json, not hidden chunks' },
  { label: 'Public delivery stays explicit', value: 'Routes and signatures in one place' },
];
