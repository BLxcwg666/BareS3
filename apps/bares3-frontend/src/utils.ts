import type { DescriptionsProps } from 'antd';
import { ApiError, type BucketInfo } from './api';
import { bucketRows as placeholderBucketRows } from './console-data';
import { sizeUnitOptions } from './constants';
import type { BucketDisplayRow, SizeUnit } from './types';

export function formatDateTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleString([], {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function formatRelativeTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  const deltaMs = parsed.getTime() - Date.now();
  const minutes = Math.round(deltaMs / 60000);
  if (Math.abs(minutes) < 60) {
    return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(minutes, 'minute');
  }

  const hours = Math.round(deltaMs / 3600000);
  if (Math.abs(hours) < 24) {
    return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(hours, 'hour');
  }

  const days = Math.round(deltaMs / 86400000);
  if (Math.abs(days) < 7) {
    return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(days, 'day');
  }

  return formatDateTime(value);
}

export function formatBytes(bytes: number) {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return '0 B';
  }
  if (bytes < 1024) {
    return `${bytes} B`;
  }

  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let index = -1;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }

  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[index]}`;
}

export function formatCount(value: number) {
  return new Intl.NumberFormat().format(value);
}

export function quotaLabel(bytes: number) {
  return bytes > 0 ? formatBytes(bytes) : 'Unlimited';
}

export function usagePercentLabel(usedBytes: number, quotaBytes: number) {
  if (quotaBytes <= 0) {
    return 'N/A';
  }

  const percent = (usedBytes / quotaBytes) * 100;
  return `${percent >= 10 ? percent.toFixed(0) : percent.toFixed(1)}%`;
}

export function usagePercentValue(usedBytes: number, quotaBytes: number) {
  if (quotaBytes <= 0) {
    return null;
  }

  return Math.max(0, Math.min(100, Math.round((usedBytes / quotaBytes) * 100)));
}

export function sizeInputToBytes(value: number | null | undefined, unit: SizeUnit) {
  if (!value || value <= 0) {
    return 0;
  }

  const selectedUnit = sizeUnitOptions.find((option) => option.value === unit) ?? sizeUnitOptions[1];
  return Math.round(value * selectedUnit.bytes);
}

export function bytesToSizeInput(bytes: number): { value?: number; unit: SizeUnit } {
  if (!bytes || bytes <= 0) {
    return { unit: 'GB' };
  }

  for (let index = sizeUnitOptions.length - 1; index >= 0; index -= 1) {
    const option = sizeUnitOptions[index];
    const amount = bytes / option.bytes;
    if (amount >= 1) {
      return {
        value: Number(amount.toFixed(amount >= 10 ? 0 : 1)),
        unit: option.value,
      };
    }
  }

  return {
    value: Number((bytes / sizeUnitOptions[0].bytes).toFixed(1)),
    unit: 'MB',
  };
}

export function safePathLabel(value: string) {
  return value || 'Not configured';
}

export function normalizeApiError(error: unknown, fallback: string) {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
}

export async function copyText(value: string) {
  if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }

  const input = document.createElement('textarea');
  input.value = value;
  input.setAttribute('readonly', 'true');
  input.style.position = 'absolute';
  input.style.left = '-9999px';
  document.body.appendChild(input);
  input.select();
  document.execCommand('copy');
  document.body.removeChild(input);
}

export function buildBucketDisplayRows(buckets: BucketInfo[]): BucketDisplayRow[] {
  if (buckets.length === 0) {
    return placeholderBucketRows.map((bucket) => ({
      name: bucket.key,
      purpose: bucket.purpose,
      root: bucket.root,
      mode: bucket.mode,
      size: bucket.size,
      objects: bucket.objects,
      fill: bucket.used ? `${bucket.used}%` : 'N/A',
      fillPercent: bucket.used ?? null,
      policy: bucket.policy,
    }));
  }

  return buckets.map((bucket) => ({
    name: bucket.name,
    purpose: bucket.quota_bytes > 0 ? `Limit ${formatBytes(bucket.quota_bytes)}` : 'Unlimited bucket quota',
    root: bucket.path,
    mode: bucket.quota_bytes > 0 ? 'Limited' : 'Unlimited',
    size: formatBytes(bucket.used_bytes),
    objects: formatCount(bucket.object_count),
    fill: usagePercentLabel(bucket.used_bytes, bucket.quota_bytes),
    fillPercent: usagePercentValue(bucket.used_bytes, bucket.quota_bytes),
    policy: bucket.metadata_layout
      ? `Metadata: ${bucket.metadata_layout} • Quota: ${quotaLabel(bucket.quota_bytes)}`
      : `Quota: ${quotaLabel(bucket.quota_bytes)}`,
  }));
}

export function nodeSummaryToItems(items: Array<{ label: string; value: string }>): DescriptionsProps['items'] {
  return items.map((item) => ({
    key: item.label,
    label: item.label,
    children: item.value,
  }));
}
