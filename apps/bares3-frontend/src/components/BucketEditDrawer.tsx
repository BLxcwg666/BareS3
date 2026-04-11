import { useEffect, useMemo, useState } from 'react';
import { App as AntApp, Button, Drawer, Form, Input, InputNumber, Select, Skeleton, Space, Switch } from 'antd';
import { useNavigate } from 'react-router-dom';
import { listBucketUsageHistory, updateBucket, type BucketInfo, type BucketUsageSample } from '../api';
import { bucketAccessModeOptions, sizeUnitOptions } from '../constants';
import type { BucketEditValues } from '../types';
import { bucketAccessModeLabel, bytesToSizeInput, formatBytes, formatCount, formatDateTime, normalizeApiError, sizeInputToBytes } from '../utils';
import { BucketUsageTrend } from './BucketUsageTrend';

function normalizeTags(tags: string[] | undefined) {
  if (!tags || tags.length === 0) {
    return [];
  }

  const seen = new Set<string>();
  return tags
    .map((tag) => tag.trim())
    .filter((tag) => tag.length > 0)
    .filter((tag) => {
      if (seen.has(tag)) {
        return false;
      }
      seen.add(tag);
      return true;
    });
}

export function BucketEditDrawer({
  bucket,
  open,
  onCancel,
  onSaved,
}: {
  bucket: BucketInfo | null;
  open: boolean;
  onCancel: () => void;
  onSaved: () => Promise<void> | void;
}) {
  const { message } = AntApp.useApp();
  const navigate = useNavigate();
  const [form] = Form.useForm<BucketEditValues>();
  const [history, setHistory] = useState<BucketUsageSample[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open || !bucket) {
      return;
    }

    const nextQuota = bytesToSizeInput(bucket.quota_bytes);
    form.setFieldsValue({
      name: bucket.name,
      accessMode: bucket.access_mode,
      replicationEnabled: bucket.replication_enabled,
      quotaValue: nextQuota.value,
      quotaUnit: nextQuota.unit,
      tags: bucket.tags ?? [],
      note: bucket.note ?? '',
    });
  }, [bucket, form, open]);

  useEffect(() => {
    if (!open || !bucket) {
      setHistory([]);
      setHistoryLoading(false);
      return;
    }

    let cancelled = false;
    setHistoryLoading(true);

    void listBucketUsageHistory(bucket.name, 24)
      .then((items) => {
        if (!cancelled) {
          setHistory(items);
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setHistory([]);
          message.error(normalizeApiError(error, 'Failed to load bucket usage trend'));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setHistoryLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [bucket, message, open]);

  const trendPoints = useMemo(() => {
    if (history.length > 0) {
      return history;
    }
    if (!bucket) {
      return [];
    }
    return [
      {
        recorded_at: bucket.created_at,
        used_bytes: bucket.used_bytes,
        object_count: bucket.object_count,
        quota_bytes: bucket.quota_bytes,
      },
    ];
  }, [bucket, history]);

  const handleSubmit = async () => {
    if (!bucket) {
      return;
    }

    let saved = false;
    const values = await form.validateFields();
    const quotaChanged = form.isFieldsTouched(['quotaValue', 'quotaUnit'], true);
    const quotaBytes = quotaChanged ? sizeInputToBytes(values.quotaValue, values.quotaUnit) : bucket.quota_bytes;
    setSubmitting(true);
    try {
      const updated = await updateBucket(bucket.name, {
        name: values.name.trim(),
        access_mode: values.accessMode,
        replication_enabled: values.replicationEnabled,
        quota_bytes: quotaBytes,
        tags: normalizeTags(values.tags),
        note: values.note.trim(),
      });
      message.success(`Bucket ${updated.name} updated`);
      onCancel();
      saved = true;
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to update bucket'));
    } finally {
      setSubmitting(false);
    }

    if (saved) {
      try {
        await onSaved();
      } catch (error) {
        message.error(normalizeApiError(error, 'Bucket updated, but refresh failed'));
      }
    }
  };

  return (
    <Drawer
      extra={
        <Space>
          <Button
            onClick={() => {
              if (submitting) {
                return;
              }
              onCancel();
            }}
          >
            Cancel
          </Button>
          <Button loading={submitting} onClick={() => void handleSubmit()} type="primary">
            Save changes
          </Button>
        </Space>
      }
      onClose={() => {
        if (submitting) {
          return;
        }
        onCancel();
      }}
      open={open}
      title={bucket ? `Edit ${bucket.name}` : 'Edit bucket'}
      width={560}
    >
      {bucket ? (
        <div className="bucket-drawer-stack">
          <div className="bucket-insight-card">
            <div className="bucket-insight-head">
              <div>
                <div className="path-label">Current usage</div>
                <div className="row-title">{bucket.quota_bytes > 0 ? `${formatBytes(bucket.used_bytes)} of ${formatBytes(bucket.quota_bytes)}` : `${formatBytes(bucket.used_bytes)} used`}</div>
              </div>
              <div className="row-note">{formatCount(bucket.object_count)} object{bucket.object_count === 1 ? '' : 's'}</div>
            </div>

            <div className="bucket-summary-grid">
              <div className="bucket-stat">
                <span className="path-label">Created</span>
                <strong className="bucket-stat-value">{formatDateTime(bucket.created_at)}</strong>
              </div>
              <div className="bucket-stat">
                <span className="path-label">Metadata</span>
                <strong className="bucket-stat-value">{bucket.metadata_layout}</strong>
              </div>
              <div className="bucket-stat">
                <span className="path-label">Access</span>
                <strong className="bucket-stat-value">{bucketAccessModeLabel(bucket.access_mode)}</strong>
              </div>
            </div>

            <div className="row-note">{bucket.path}</div>
          </div>

            <Form form={form} initialValues={{ accessMode: 'private', replicationEnabled: false, note: '', quotaUnit: 'GB', tags: [] }} layout="vertical">
              <Form.Item label="Bucket name" name="name" rules={[{ required: true, whitespace: true, message: 'Bucket name is required' }]}>
                <Input placeholder="gallery" />
              </Form.Item>

              <Form.Item
                extra="Private requires auth, Public enables /pub, and Custom uses the access rules page."
                label="Access mode"
                name="accessMode"
                rules={[{ required: true, message: 'Access mode is required' }]}
              >
                <Select options={bucketAccessModeOptions} />
              </Form.Item>

              <Form.Item extra="This bucket only syncs when both global replication and this switch are on." label="Replication" name="replicationEnabled" valuePropName="checked">
                <Switch checkedChildren="On" unCheckedChildren="Off" />
              </Form.Item>

              {bucket ? (
                <Button
                  onClick={() => {
                    onCancel();
                    navigate(`/buckets/${encodeURIComponent(bucket.name)}/access`);
                  }}
                  size="small"
                  type="link"
                >
                  Open access rules page
                </Button>
              ) : null}

              <Form.Item extra="Leave empty or 0 for unlimited." label="Bucket limit">
                <Space.Compact block>
                  <Form.Item name="quotaValue" noStyle>
                    <InputNumber min={0} placeholder="Unlimited" precision={3} style={{ width: '100%' }} />
                  </Form.Item>
                  <Form.Item name="quotaUnit" noStyle>
                    <Select options={sizeUnitOptions.map((option) => ({ label: option.label, value: option.value }))} style={{ width: 96 }} />
                  </Form.Item>
                </Space.Compact>
              </Form.Item>

              <Form.Item extra="Press Enter or comma to add a label." label="Labels" name="tags">
                <Select mode="tags" open={false} placeholder="media, launch" tokenSeparators={[',']} />
              </Form.Item>

              <Form.Item extra="Helpful context for teammates scanning this bucket list." label="Note" name="note">
                <Input.TextArea autoSize={{ minRows: 3, maxRows: 6 }} placeholder="Launch files mirrored from the CDN build." />
              </Form.Item>
            </Form>

            <div className="bucket-trend-panel">
              <div className="bucket-panel-head">
                <div>
                  <div className="path-label">Usage trend</div>
                  <div className="row-title">Recent bucket change snapshots</div>
                </div>
                <div className="row-note">Uploads, deletes, moves, and quota edits append a new point.</div>
              </div>

              {historyLoading ? <Skeleton active paragraph={{ rows: 4 }} title={false} /> : <BucketUsageTrend points={trendPoints} />}
            </div>
          </div>
        ) : null}
    </Drawer>
  );
}
