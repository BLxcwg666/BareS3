import { ApiError, buildApiUrl, request } from './client';

const multipartUploadThresholdBytes = 16 * 1024 * 1024;
const multipartUploadChunkBytes = 16 * 1024 * 1024;

export type UploadObjectOptions = {
  onProgress?: (uploadedBytes: number) => void;
};

function requestWithUploadProgress<T>(path: string, init: RequestInit, onProgress?: (loadedBytes: number) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open(init.method ?? 'GET', buildApiUrl(path));
    xhr.withCredentials = true;

    const headers = new Headers({
      Accept: 'application/json',
      ...(init.headers ?? {}),
    });
    headers.forEach((value, key) => {
      xhr.setRequestHeader(key, value);
    });

    if (onProgress) {
      xhr.upload.onprogress = (event) => {
        onProgress(event.loaded);
      };
    }

    xhr.onload = () => {
      const responseText = xhr.responseText ?? '';
      const contentType = xhr.getResponseHeader('content-type') ?? '';
      const body = contentType.includes('application/json') ? JSON.parse(responseText || 'null') : responseText;

      if (xhr.status === 204) {
        resolve(undefined as T);
        return;
      }

      if (xhr.status < 200 || xhr.status >= 300) {
        const message =
          typeof body === 'object' && body !== null && 'message' in body
            ? String(body.message)
            : `Request failed with status ${xhr.status}`;
        reject(new ApiError(message, xhr.status));
        return;
      }

      resolve(body as T);
    };

    xhr.onerror = () => {
      reject(new ApiError('Network request failed', xhr.status || 0));
    };

    xhr.send((init.body ?? null) as Document | XMLHttpRequestBodyInit | null);
  });
}

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
    domain_bindings: PublicDomainBinding[];
    max_bytes: number;
    used_bytes: number;
    bucket_count: number;
    active_link_count: number;
  };
  sync: {
    enabled: boolean;
  };
};

export type SystemSettings = {
  public_base_url: string;
  s3_base_url: string;
  region: string;
  metadata_layout: string;
  tmp_dir: string;
};

export type PublicDomainBinding = {
  host: string;
  bucket: string;
  prefix?: string;
  index_document?: boolean;
  spa_fallback?: boolean;
};

export type DomainSettings = {
  items: PublicDomainBinding[];
};

export type SyncStatusState = 'pending' | 'verifying' | 'downloading' | 'ready' | 'error' | 'conflict';

export type SyncObjectStatus = {
  bucket: string;
  key: string;
  status: SyncStatusState;
  expected_checksum_sha256?: string;
  last_error?: string;
  updated_at: string;
};

export type SyncSettings = {
  local_node_id?: string;
  enabled: boolean;
  role: string;
  leader_url: string;
  shared_secret: string;
  poll_interval_seconds: number;
  leader_cursor?: number;
  applied_cursor?: number;
  reconcile_counts?: {
    pending: number;
    verifying: number;
    downloading: number;
    ready: number;
    error: number;
    conflict: number;
  };
  reconcile_summary?: {
    baseline_node_id?: string;
    last_error?: string;
  };
  conflict_items?: Array<{
    bucket: string;
    key: string;
    source?: string;
    baseline_node_id?: string;
    last_error?: string;
    updated_at: string;
  }>;
};

export type SyncAccessToken = {
  id: string;
  token: string;
  label?: string;
  created_by?: string;
  created_at: string;
  status: 'active' | 'revoked';
  revoked_at?: string;
};

export type ReplicationRemote = {
  id: string;
  display_name: string;
  endpoint: string;
  enabled: boolean;
  follow_changes: boolean;
  status: 'pending' | 'syncing' | 'idle' | 'error';
  connection_status: 'disconnected' | 'connecting' | 'connected';
  bootstrap_mode: 'full' | 'from_now';
  cursor: number;
  last_error?: string;
  last_sync_started_at?: string;
  last_heartbeat_at?: string;
  last_sync_at?: string;
  peer_cursor: number;
  peer_used_bytes: number;
  peer_bucket_count: number;
  peer_object_count: number;
  objects_total: number;
  objects_completed: number;
  bytes_total: number;
  bytes_completed: number;
  download_rate_bps: number;
  upload_rate_bps: number;
  sync_counts?: {
    pending: number;
    verifying: number;
    downloading: number;
    ready: number;
    error: number;
    conflict: number;
  };
  created_at: string;
  updated_at: string;
};

export type BucketAccessMode = 'private' | 'public' | 'custom';

export type BucketAccessAction = 'public' | 'authenticated' | 'deny';

export type BucketAccessRule = {
  prefix: string;
  action: BucketAccessAction;
  note?: string;
};

export type BucketAccessPolicy = {
  default_action: BucketAccessAction;
  rules: BucketAccessRule[];
};

export type BucketAccessConfig = {
  mode: BucketAccessMode;
  policy: BucketAccessPolicy;
};

export type BucketInfo = {
  name: string;
  path: string;
  metadata_path: string;
  created_at: string;
  metadata_layout: string;
  access_mode: BucketAccessMode;
  replication_enabled: boolean;
  quota_bytes: number;
  tags?: string[];
  note?: string;
  used_bytes: number;
  object_count: number;
};

export type S3CredentialPermission = 'read_write' | 'read_only';

export type S3CredentialInfo = {
  access_key_id: string;
  label?: string;
  source?: string;
  permission: S3CredentialPermission;
  buckets: string[];
  created_at: string;
  last_used_at?: string;
  revoked_at?: string;
  status: 'active' | 'revoked';
};

export type CreatedS3Credential = S3CredentialInfo & {
  secret_access_key: string;
};

export type BucketUsageSample = {
  recorded_at: string;
  used_bytes: number;
  object_count: number;
  quota_bytes: number;
};

export type UpdateBucketPayload = {
  name: string;
  access_mode: BucketAccessMode;
  replication_enabled: boolean;
  quota_bytes: number;
  tags: string[];
  note: string;
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
  sync_status?: SyncObjectStatus;
};

type MultipartUploadInfo = {
  upload_id: string;
  bucket: string;
  key: string;
  content_type?: string;
};

type MultipartPartInfo = {
  part_number: number;
  etag: string;
  size: number;
  last_modified: string;
};

export type ListObjectsOptions = {
  prefix?: string;
  query?: string;
  limit?: number;
  cursor?: string;
  offset?: number;
  delimiter?: string;
};

export type ListObjectsResult = {
  items: ObjectInfo[];
  prefixes?: string[];
  total_count?: number;
  has_more: boolean;
  next_cursor?: string;
};

export type SearchHit = {
  kind: 'bucket' | 'object';
  bucket: string;
  key?: string;
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

export type UpdateObjectMetadataPayload = {
  content_type: string;
  content_disposition: string;
  cache_control: string;
  user_metadata: Record<string, string>;
};

export function getRuntime() {
  return request<RuntimeInfo>('/api/v1/runtime');
}

export async function listBuckets() {
  const payload = await request<{ items: BucketInfo[] }>('/api/v1/buckets');
  return payload.items;
}

export function getBucketAccessConfig(bucket: string) {
  return request<BucketAccessConfig>(`/api/v1/buckets/${encodeURIComponent(bucket)}/access`);
}

export function updateBucketAccessConfig(bucket: string, payload: BucketAccessConfig) {
  return request<BucketAccessConfig>(`/api/v1/buckets/${encodeURIComponent(bucket)}/access`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}

export function createBucket(name: string, quotaBytes = 0, accessMode: BucketAccessMode = 'private', replicationEnabled = false) {
  return request<BucketInfo>('/api/v1/buckets', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ name, access_mode: accessMode, replication_enabled: replicationEnabled, quota_bytes: quotaBytes }),
  });
}

export function deleteBucket(bucket: string) {
  return request<void>(`/api/v1/buckets/${encodeURIComponent(bucket)}`, {
    method: 'DELETE',
  });
}

export function updateBucket(bucket: string, payload: UpdateBucketPayload) {
  return request<BucketInfo>(`/api/v1/buckets/${encodeURIComponent(bucket)}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}

export async function listBucketUsageHistory(bucket: string, limit = 24) {
  const payload = await request<{ items: BucketUsageSample[] }>(
    `/api/v1/buckets/${encodeURIComponent(bucket)}/history?limit=${limit}`,
  );
  return payload.items;
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

export function getSystemSettings() {
  return request<SystemSettings>('/api/v1/settings/system');
}

export function updateSystemSettings(payload: SystemSettings) {
  return request<SystemSettings>('/api/v1/settings/system', {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}

export function getDomainSettings() {
  return request<DomainSettings>('/api/v1/settings/domains');
}

export function updateDomainSettings(payload: DomainSettings) {
  return request<DomainSettings>('/api/v1/settings/domains', {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}

export function getSyncSettings() {
  return request<SyncSettings>('/api/v1/settings/sync');
}

export function updateSyncSettings(payload: { enabled: boolean }) {
  return request<SyncSettings>('/api/v1/settings/sync', {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}

export async function listReplicationTokens() {
  const payload = await request<{ items: SyncAccessToken[] }>('/api/v1/replication/tokens');
  return payload.items;
}

export function createReplicationToken(label = '') {
  return request<SyncAccessToken>('/api/v1/replication/tokens', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ label }),
  });
}

export function revokeReplicationToken(id: string) {
  return request<SyncAccessToken>(`/api/v1/replication/tokens/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  });
}

export function deleteReplicationToken(id: string) {
  return request<SyncAccessToken>(`/api/v1/replication/tokens/${encodeURIComponent(id)}/remove`, {
    method: 'DELETE',
  });
}

export async function listReplicationRemotes() {
  const payload = await request<{ items: ReplicationRemote[] }>('/api/v1/replication/remotes');
  return payload.items;
}

export function createReplicationRemote(payload: {
  display_name: string;
  endpoint: string;
  token: string;
  enabled?: boolean;
  follow_changes: boolean;
  bootstrap_mode: 'full' | 'from_now';
}) {
  return request<ReplicationRemote>('/api/v1/replication/remotes', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}

export function updateReplicationRemote(id: string, payload: {
  display_name?: string;
  endpoint?: string;
  token?: string;
  bootstrap_mode?: 'full' | 'from_now';
  enabled?: boolean;
  follow_changes?: boolean;
}) {
  return request<ReplicationRemote>(`/api/v1/replication/remotes/${encodeURIComponent(id)}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}

export function deleteReplicationRemote(id: string) {
  return request<ReplicationRemote>(`/api/v1/replication/remotes/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  });
}

export function resolveSyncConflict(bucket: string, key: string) {
  return request<{ status: string }>('/api/v1/settings/sync/conflicts/resolve', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ bucket, key }),
  });
}

export async function listS3Credentials() {
  const payload = await request<{ items: S3CredentialInfo[] }>('/api/v1/settings/s3/credentials');
  return payload.items;
}

export function createS3Credential(label: string, permission: S3CredentialPermission, buckets: string[]) {
  return request<CreatedS3Credential>('/api/v1/settings/s3/credentials', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ label, permission, buckets }),
  });
}

export function updateS3Credential(accessKeyID: string, label: string, permission: S3CredentialPermission, buckets: string[]) {
  return request<S3CredentialInfo>(`/api/v1/settings/s3/credentials/${encodeURIComponent(accessKeyID)}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ label, permission, buckets }),
  });
}

export function revokeS3Credential(accessKeyID: string) {
  return request<S3CredentialInfo>(`/api/v1/settings/s3/credentials/${encodeURIComponent(accessKeyID)}`, {
    method: 'DELETE',
  });
}

export function removeS3Credential(accessKeyID: string) {
  return request<void>(`/api/v1/settings/s3/credentials/${encodeURIComponent(accessKeyID)}/remove`, {
    method: 'DELETE',
  });
}

export async function listObjects(bucket: string, options: ListObjectsOptions = {}) {
  const query = new URLSearchParams();
  if (options.prefix?.trim()) {
    query.set('prefix', options.prefix.trim());
  }
  if (options.query?.trim()) {
    query.set('query', options.query.trim());
  }
  if (options.cursor?.trim()) {
    query.set('cursor', options.cursor.trim());
  }
  if (typeof options.offset === 'number' && options.offset > 0) {
    query.set('offset', String(options.offset));
  }
  if (typeof options.limit === 'number' && options.limit > 0) {
    query.set('limit', String(options.limit));
  }
  if (options.delimiter?.trim()) {
    query.set('delimiter', options.delimiter.trim());
  }

  const suffix = query.toString();
  return request<ListObjectsResult>(
    `/api/v1/buckets/${encodeURIComponent(bucket)}/objects${suffix ? `?${suffix}` : ''}`,
  );
}

export async function searchStorage(query: string, limit = 12) {
  const params = new URLSearchParams();
  if (query.trim()) {
    params.set('query', query.trim());
  }
  if (limit > 0) {
    params.set('limit', String(limit));
  }

  const suffix = params.toString();
  const payload = await request<{ items: SearchHit[] }>(`/api/v1/search${suffix ? `?${suffix}` : ''}`);
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

export function updateObjectMetadata(bucket: string, key: string, payload: UpdateObjectMetadataPayload) {
  return request<ObjectInfo>(`/api/v1/buckets/${encodeURIComponent(bucket)}/metadata/${encodeObjectKeyPath(key)}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
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

async function uploadMultipartObject(bucket: string, file: File, key: string, options?: UploadObjectOptions) {
  const upload = await request<MultipartUploadInfo>(`/api/v1/buckets/${encodeURIComponent(bucket)}/uploads`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      key,
      content_type: file.type,
    }),
  });

  const parts: Array<{ part_number: number; etag: string }> = [];

  try {
    let uploadedBytes = 0;
    let partNumber = 1;
    for (let offset = 0; offset < file.size; offset += multipartUploadChunkBytes) {
      const chunk = file.slice(offset, Math.min(file.size, offset + multipartUploadChunkBytes));
      const part = await requestWithUploadProgress<MultipartPartInfo>(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/uploads/${encodeURIComponent(upload.upload_id)}/parts/${partNumber}?key=${encodeURIComponent(key)}`,
        {
          method: 'PUT',
          headers: {
            'Content-Type': 'application/octet-stream',
          },
          body: chunk,
        },
        (loadedBytes) => {
          options?.onProgress?.(Math.min(file.size, uploadedBytes + Math.min(loadedBytes, chunk.size)));
        },
      );
      parts.push({ part_number: part.part_number, etag: part.etag });
      uploadedBytes += chunk.size;
      options?.onProgress?.(uploadedBytes);
      partNumber += 1;
    }

    return request<ObjectInfo>(`/api/v1/buckets/${encodeURIComponent(bucket)}/uploads/${encodeURIComponent(upload.upload_id)}/complete`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ key, parts }),
    });
  } catch (error) {
    try {
      await request<void>(`/api/v1/buckets/${encodeURIComponent(bucket)}/uploads/${encodeURIComponent(upload.upload_id)}?key=${encodeURIComponent(key)}`, {
        method: 'DELETE',
      });
    } catch {
      // Ignore abort errors and surface the original upload failure.
    }
    throw error;
  }
}

export function uploadObject(bucket: string, file: File, key?: string, options?: UploadObjectOptions) {
  const resolvedKey = key?.trim() || file.name.trim();
  if (file.size >= multipartUploadThresholdBytes && resolvedKey) {
    return uploadMultipartObject(bucket, file, resolvedKey, options);
  }

  const formData = new FormData();
  formData.append('file', file);
  if (key?.trim()) {
    formData.append('key', key.trim());
  }

  return requestWithUploadProgress<ObjectInfo>(
    `/api/v1/buckets/${encodeURIComponent(bucket)}/objects`,
    {
      method: 'POST',
      body: formData,
    },
    (loadedBytes) => {
      options?.onProgress?.(Math.min(file.size, loadedBytes));
    },
  );
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

export function deleteBrowserPrefix(bucket: string, prefix: string) {
  return request<{ deleted_count: number }>('/api/v1/browser/delete', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      kind: 'prefix',
      bucket,
      prefix,
    }),
  });
}
