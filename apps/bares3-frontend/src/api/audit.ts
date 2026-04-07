import { request } from './client';

export type AuditEntry = {
  time: string;
  actor: string;
  action: string;
  title: string;
  detail?: string;
  target?: string;
  remote?: string;
  status?: string;
};

export async function listAuditEntries(limit = 10) {
  const payload = await request<{ items: AuditEntry[] }>(`/api/v1/audit/events?limit=${limit}`);
  return payload.items;
}
