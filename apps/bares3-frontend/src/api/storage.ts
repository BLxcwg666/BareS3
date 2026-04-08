import { request } from './client';

function encodeObjectKeyPath(key: string) {
  return key
    .split('/')
    .map((part) => encodeURIComponent(part))
    .join('/');
}

export type RuntimeInfo = {
  app: {
    name: string;
    env: string;
  };
  version: {
    product: string;
    version: string;
    commit: string;
    built_at: string;
    go_version: string;
  };
  config: {
    path: string;
    used: boolean;
    base: string;
  };
  paths: {
    data_dir: string;
    log_dir: string;
    tmp_dir: string;
  };
  listen: {
    admin: string;
    s3: string;
    file: string;
  };
  storage: {
    region: string;
    public_base_url: string;
    s3_base_url: string;
    metadata_layout: string;
    max_bytes: number;
    used_bytes: number;
    bucket_count: number;
    active_link_count: number;
  };
};

export type BucketInfo = {
  name: string;
  path: string;
  metadata_path: string;
  created_at: string;
  metadata_layout: string;
  quota_bytes: number;
  used_bytes: number;
  object_count: number;
};

export type ObjectInfo = {
  bucket: string;
  key: string;
  path: string;
  metadata_path: string;
  size: number;
  etag: string;
  content_type: string;
  cache_control?: string;
  content_disposition?: string;
  user_metadata?: Record<string, string>;
  last_modified: string;
};

export type PresignResult = {
  url: string;
  expires_at: string;
  method: string;
};

export type MoveEntryRequest =
  | {
      kind: 'object';
      source_bucket: string;
      source_key: string;
      destination_bucket: string;
      destination_key: string;
    }
  | {
      kind: 'prefix';
      source_bucket: string;
      source_prefix: string;
      destination_bucket: string;
      destination_prefix: string;
    };

export type MoveEntryResult = {
  kind: 'object' | 'prefix';
  source_bucket: string;
  source_key?: string;
  source_prefix?: string;
  destination_bucket: string;
  destination_key?: string;
  destination_prefix?: string;
  moved_count: number;
};

export function getRuntime() {
  return request<RuntimeInfo>('/api/v1/runtime');
}

export async function listBuckets() {
  const payload = await request<{ items: BucketInfo[] }>('/api/v1/buckets');
  return payload.items;
}

export function createBucket(name: string, quotaBytes = 0) {
  return request<BucketInfo>('/api/v1/buckets', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ name, quota_bytes: quotaBytes }),
  });
}

export function deleteBucket(bucket: string) {
  return request<void>(`/api/v1/buckets/${encodeURIComponent(bucket)}`, {
    method: 'DELETE',
  });
}

export function updateStorageLimit(maxBytes: number) {
  return request<{ max_bytes: number }>('/api/v1/settings/storage', {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ max_bytes: maxBytes }),
  });
}

export async function listObjects(bucket: string, prefix?: string, limit?: number) {
  const query = new URLSearchParams();
  if (prefix?.trim()) {
    query.set('prefix', prefix.trim());
  }
  if (typeof limit === 'number' && limit > 0) {
    query.set('limit', String(limit));
  }

  const suffix = query.toString();
  const payload = await request<{ items: ObjectInfo[] }>(
    `/api/v1/buckets/${encodeURIComponent(bucket)}/objects${suffix ? `?${suffix}` : ''}`,
  );
  return payload.items;
}

export function getObject(bucket: string, key: string) {
  return request<ObjectInfo>(`/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeObjectKeyPath(key)}`);
}

export function deleteObject(bucket: string, key: string) {
  return request<void>(`/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeObjectKeyPath(key)}`, {
    method: 'DELETE',
  });
}

export function presignObject(bucket: string, key: string, expiresSeconds = 900, method = 'GET') {
  return request<PresignResult>('/api/v1/presign/s3', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      method,
      bucket,
      key,
      expires_seconds: expiresSeconds,
    }),
  });
}

export function uploadObject(bucket: string, file: File, key?: string) {
  const formData = new FormData();
  formData.append('file', file);
  if (key?.trim()) {
    formData.append('key', key.trim());
  }

  return request<ObjectInfo>(`/api/v1/buckets/${encodeURIComponent(bucket)}/objects`, {
    method: 'POST',
    body: formData,
  });
}

export function moveBrowserEntry(payload: MoveEntryRequest) {
  return request<MoveEntryResult>('/api/v1/browser/move', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}
