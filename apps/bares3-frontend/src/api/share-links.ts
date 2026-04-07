import { request } from './client';

export type ShareLinkStatus = 'active' | 'expired' | 'revoked';

export type ShareLinkInfo = {
  id: string;
  bucket: string;
  key: string;
  filename: string;
  content_type?: string;
  size: number;
  created_by?: string;
  created_at: string;
  expires_at: string;
  revoked_at?: string;
  status: ShareLinkStatus;
  url: string;
  download_url: string;
};

export async function listShareLinks() {
  const payload = await request<{ items: ShareLinkInfo[] }>('/api/v1/share-links');
  return payload.items;
}

export function createShareLink(bucket: string, key: string, expiresSeconds = 86400) {
  return request<ShareLinkInfo>('/api/v1/share-links', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      bucket,
      key,
      expires_seconds: expiresSeconds,
    }),
  });
}

export function revokeShareLink(id: string) {
  return request<ShareLinkInfo>(`/api/v1/share-links/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  });
}
