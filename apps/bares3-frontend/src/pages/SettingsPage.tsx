import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { App, Button, Collapse, Empty, Input, InputNumber, Select, Skeleton, Space, Typography } from 'antd';
import { type S3CredentialInfo, updateStorageLimit, updateSystemSettings } from '../api';
import { ConsoleShell } from '../components/ConsoleShell';
import { S3CredentialModal } from '../components/S3CredentialModal';
import { Section } from '../components/Section';
import { sizeUnitOptions } from '../constants';
import { useBucketsData } from '../hooks/useBucketsData';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { useS3CredentialsData } from '../hooks/useS3CredentialsData';
import type { SizeUnit } from '../types';
import { bytesToSizeInput, formatBytes, formatCount, normalizeApiError, quotaLabel, sizeInputToBytes } from '../utils';

const { Text } = Typography;

function SettingsField({ label, hint, action, children, surface = true }: { label: string; hint?: string; action?: ReactNode; children: ReactNode; surface?: boolean }) {
  return (
      <div className="settings-field" style={{ alignItems: 'flex-start' }}>
        <div className="settings-field-main" style={{ paddingTop: 0, transform: 'translateY(-3px)' }}>
          <div className="settings-field-label">{label}</div>
          {hint ? <Text className="settings-field-hint">{hint}</Text> : null}
        </div>
        <div className="settings-field-body">
          <div className={surface ? 'settings-field-surface' : 'settings-field-control'}>{children}</div>
          {action}
        </div>
      </div>
  );
}

export function SettingsPage() {
  const { message } = App.useApp();
  const { runtime, loading: runtimeLoading, refresh: refreshRuntime } = useRuntimeData();
  const { items: buckets } = useBucketsData();
  const { refresh: refreshCredentials } = useS3CredentialsData();

  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [editingCredential, setEditingCredential] = useState<S3CredentialInfo | null>(null);
  const [saving, setSaving] = useState(false);
  const [systemSavingHint, setSystemSavingHint] = useState('');
  const [formState, setFormState] = useState({
    publicBaseURL: '',
    s3BaseURL: '',
    region: '',
    metadataLayout: '',
    tmpDir: '',
    maxValue: null as number | null,
    maxUnit: 'GB' as SizeUnit,
  });

  const bucketNames = useMemo(() => buckets.map((bucket) => bucket.name), [buckets]);
  const maxBytes = runtime?.storage.max_bytes ?? 0;
  const usedBytes = runtime?.storage.used_bytes ?? 0;

  useEffect(() => {
    if (!runtime) {
      return;
    }
    setFormState({
      publicBaseURL: runtime.storage.public_base_url ?? '',
      s3BaseURL: runtime.storage.s3_base_url ?? '',
      region: runtime.storage.region ?? '',
      metadataLayout: runtime.storage.metadata_layout ?? '',
      tmpDir: runtime.paths.tmp_dir ?? '',
      maxValue: bytesToSizeInput(runtime.storage.max_bytes ?? 0).value ?? null,
      maxUnit: bytesToSizeInput(runtime.storage.max_bytes ?? 0).unit,
    });
    setSystemSavingHint('');
  }, [runtime]);

  const limitBytes = sizeInputToBytes(formState.maxValue, formState.maxUnit);

  const dirty = runtime
    ? formState.publicBaseURL !== (runtime.storage.public_base_url ?? '')
      || formState.s3BaseURL !== (runtime.storage.s3_base_url ?? '')
      || formState.region !== (runtime.storage.region ?? '')
      || formState.metadataLayout !== (runtime.storage.metadata_layout ?? '')
      || formState.tmpDir !== (runtime.paths.tmp_dir ?? '')
      || limitBytes !== (runtime.storage.max_bytes ?? 0)
    : false;

  const handleSave = async () => {
    setSaving(true);
    try {
      const [systemResult] = await Promise.all([
        updateSystemSettings({
          public_base_url: formState.publicBaseURL.trim(),
          s3_base_url: formState.s3BaseURL.trim(),
          region: formState.region.trim(),
          metadata_layout: formState.metadataLayout.trim(),
          tmp_dir: formState.tmpDir.trim(),
        }),
        updateStorageLimit(limitBytes),
      ]);
      await refreshRuntime();
      setSystemSavingHint(systemResult.tmp_dir.trim() !== (runtime?.paths.tmp_dir ?? '').trim() ? 'Temp directory was saved to config and takes effect after restart.' : '');
      message.success('Settings saved');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to save settings'));
    } finally {
      setSaving(false);
    }
  };

  const systemSummary = runtimeLoading
    ? 'Loading system settings'
    : !runtime
      ? 'System settings unavailable'
      : `${runtime.storage.region || 'No region'} · ${runtime.paths.tmp_dir || 'No temp directory'}`;

  const storageSummary = runtimeLoading
    ? 'Loading storage settings'
    : !runtime
      ? 'Runtime settings unavailable'
      : `${quotaLabel(maxBytes)} limit · ${formatBytes(usedBytes)} used · ${formatCount(runtime.storage.bucket_count)} bucket${runtime.storage.bucket_count === 1 ? '' : 's'}`;

  const panelItems = [
    {
      key: 'system',
      label: (
        <div className="settings-category-header">
          <div className="settings-category-label">System</div>
          <Text className="settings-category-summary">{systemSummary}</Text>
        </div>
      ),
      children: runtimeLoading ? (
        <Skeleton active paragraph={{ rows: 6 }} title={false} />
      ) : !runtime ? (
        <div className="settings-empty">
          <Empty description="System settings are unavailable" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        </div>
      ) : (
        <div className="settings-detail-stack">
          <div className="settings-detail-section">
            <div className="settings-field-list">
              <SettingsField hint="Base URL used for public object links and downloads." label="Public base URL" surface={false}>
                <Input
                  onChange={(event) => setFormState((current) => ({ ...current, publicBaseURL: event.target.value }))}
                  value={formState.publicBaseURL}
                  style={{ width: '100%' }}
                />
              </SettingsField>
              <SettingsField hint="Base URL used for S3 API requests and presigned URLs." label="S3 base URL" surface={false}>
                <Input
                  onChange={(event) => setFormState((current) => ({ ...current, s3BaseURL: event.target.value }))}
                  value={formState.s3BaseURL}
                  style={{ width: '100%' }}
                />
              </SettingsField>
              <SettingsField hint="AWS-compatible region value used for request signing." label="Region" surface={false}>
                <Input
                  onChange={(event) => setFormState((current) => ({ ...current, region: event.target.value }))}
                  value={formState.region}
                  style={{ width: '100%' }}
                />
              </SettingsField>
              <SettingsField hint="Metadata storage layout used for bucket and object metadata." label="Metadata layout" surface={false}>
                <Input
                  onChange={(event) => setFormState((current) => ({ ...current, metadataLayout: event.target.value }))}
                  value={formState.metadataLayout}
                  style={{ width: '100%' }}
                />
              </SettingsField>
              <SettingsField hint="Temporary workspace used for atomic writes and uploads. Changing this is saved to config and applies after restart." label="Temp directory" surface={false}>
                <Input
                  onChange={(event) => setFormState((current) => ({ ...current, tmpDir: event.target.value }))}
                  value={formState.tmpDir}
                  style={{ width: '100%' }}
                />
              </SettingsField>
            </div>
          </div>
          {systemSavingHint ? <Text type="secondary">{systemSavingHint}</Text> : null}
        </div>
      ),
    },
    {
      key: 'storage',
      label: (
        <div className="settings-category-header">
          <div className="settings-category-label">Storage</div>
          <Text className="settings-category-summary">{storageSummary}</Text>
        </div>
      ),
      children: runtimeLoading ? (
        <Skeleton active paragraph={{ rows: 6 }} title={false} />
      ) : !runtime ? (
        <div className="settings-empty">
          <Empty description="Storage settings are unavailable" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        </div>
      ) : (
        <div className="settings-detail-stack">
          <div className="settings-detail-section">
            <div className="settings-field-list">
              <SettingsField hint="Total bytes this node may consume across bucket data and metadata." label="Space limit">
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <Space.Compact>
                    <InputNumber
                      min={0}
                      onChange={(value) => setFormState((current) => ({ ...current, maxValue: value }))}
                      placeholder="Unlimited"
                      precision={1}
                      style={{ width: 180 }}
                      value={formState.maxValue}
                    />
                    <Select
                      onChange={(value) => setFormState((current) => ({ ...current, maxUnit: value }))}
                      options={sizeUnitOptions.map((option) => ({ label: option.label, value: option.value }))}
                      style={{ width: 96 }}
                      value={formState.maxUnit}
                    />
                  </Space.Compact>
                  <Text type="secondary">
                    {limitBytes > 0 ? quotaLabel(limitBytes) : 'Unlimited'} / {formatBytes(usedBytes)} used
                  </Text>
                </div>
              </SettingsField>
            </div>
          </div>
        </div>
      ),
    }
  ];

  return (
    <ConsoleShell>
      <div className="workspace-stack">
        <S3CredentialModal bucketNames={bucketNames} credential={null} onCancel={() => setIsCreateModalOpen(false)} onSaved={() => refreshCredentials()} open={isCreateModalOpen} />
        <S3CredentialModal bucketNames={bucketNames} credential={editingCredential} onCancel={() => setEditingCredential(null)} onSaved={() => refreshCredentials()} open={Boolean(editingCredential)} />

        <Section flush title="Settings" extra={<Button disabled={!dirty || runtimeLoading || !runtime} loading={saving} onClick={() => void handleSave()} type="primary">Save</Button>}>
          <Collapse accordion className="settings-collapse" defaultActiveKey={['system']} expandIconPosition="start" items={panelItems} />
        </Section>
      </div>
    </ConsoleShell>
  );
}
