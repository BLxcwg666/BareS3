export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

export type AuthSession = {
  username: string;
  expires_at: string;
};

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

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL?.trim() ?? '';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    credentials: 'include',
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init?.headers ?? {}),
    },
  });

  if (response.status === 204) {
    return undefined as T;
  }

  const contentType = response.headers.get('content-type') ?? '';
  const body = contentType.includes('application/json')
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const message =
      typeof body === 'object' && body !== null && 'message' in body
        ? String(body.message)
        : `Request failed with status ${response.status}`;
    throw new ApiError(message, response.status);
  }

  return body as T;
}

export function getSession() {
  return request<AuthSession>('/api/v1/auth/me');
}

export function login(username: string, password: string) {
  return request<AuthSession>('/api/v1/auth/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ username, password }),
  });
}

export function logout() {
  return request<void>('/api/v1/auth/logout', {
    method: 'POST',
  });
}

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

export function updateStorageLimit(maxBytes: number) {
  return request<{ max_bytes: number }>('/api/v1/settings/storage', {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ max_bytes: maxBytes }),
  });
}

export async function listObjects(bucket: string, prefix?: string) {
  const query = new URLSearchParams();
  if (prefix?.trim()) {
    query.set('prefix', prefix.trim());
  }

  const suffix = query.toString();
  const payload = await request<{ items: ObjectInfo[] }>(
    `/api/v1/buckets/${encodeURIComponent(bucket)}/objects${suffix ? `?${suffix}` : ''}`,
  );
  return payload.items;
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
