import { useMemo, useState, type ReactNode } from 'react';
import { Button, Collapse, Empty, Skeleton, Typography } from 'antd';
import { type S3CredentialInfo } from '../api';
import { ConsoleShell } from '../components/ConsoleShell';
import { S3CredentialModal } from '../components/S3CredentialModal';
import { Section } from '../components/Section';
import { StorageLimitModal } from '../components/StorageLimitModal';
import { useBucketsData } from '../hooks/useBucketsData';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { useS3CredentialsData } from '../hooks/useS3CredentialsData';
import { formatBytes, formatCount, quotaLabel } from '../utils';

const { Text } = Typography;

function SettingsField({ label, hint, action, children }: { label: string; hint?: string; action?: ReactNode; children: ReactNode }) {
  return (
      <div className="settings-field" style={{ alignItems: 'flex-start' }}>
        <div className="settings-field-main" style={{ paddingTop: 0, transform: 'translateY(-3px)' }}>
          <div className="settings-field-label">{label}</div>
          {hint ? <Text className="settings-field-hint">{hint}</Text> : null}
        </div>
        <div className="settings-field-body">
          <div className="settings-field-surface">{children}</div>
          {action}
        </div>
      </div>
  );
}

export function SettingsPage() {
  const { runtime, loading: runtimeLoading, refresh: refreshRuntime } = useRuntimeData();
  const { items: buckets } = useBucketsData();
  const { refresh: refreshCredentials } = useS3CredentialsData();

  const [isStorageModalOpen, setIsStorageModalOpen] = useState(false);
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [editingCredential, setEditingCredential] = useState<S3CredentialInfo | null>(null);

  const bucketNames = useMemo(() => buckets.map((bucket) => bucket.name), [buckets]);
  const maxBytes = runtime?.storage.max_bytes ?? 0;
  const usedBytes = runtime?.storage.used_bytes ?? 0;

  const storageSummary = runtimeLoading
    ? 'Loading storage settings'
    : !runtime
      ? 'Runtime settings unavailable'
      : `${quotaLabel(maxBytes)} limit · ${formatBytes(usedBytes)} used · ${formatCount(runtime.storage.bucket_count)} bucket${runtime.storage.bucket_count === 1 ? '' : 's'}`;

  const panelItems = [
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
              <SettingsField
                  action={
                    <Button onClick={() => setIsStorageModalOpen(true)}>
                      Edit limit
                    </Button>
                  }
                  hint="Total bytes this node may consume across bucket data and metadata."
                  label="Space limit"
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <Text strong>
                    {maxBytes > 0 ? quotaLabel(maxBytes) : 'Unlimited'}
                  </Text>
                  <Text type="secondary">
                    / {formatBytes(usedBytes)} used
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
        <StorageLimitModal
          currentMaxBytes={maxBytes}
          onCancel={() => setIsStorageModalOpen(false)}
          onSaved={() => refreshRuntime()}
          open={isStorageModalOpen}
        />
        <S3CredentialModal bucketNames={bucketNames} credential={null} onCancel={() => setIsCreateModalOpen(false)} onSaved={() => refreshCredentials()} open={isCreateModalOpen} />
        <S3CredentialModal bucketNames={bucketNames} credential={editingCredential} onCancel={() => setEditingCredential(null)} onSaved={() => refreshCredentials()} open={Boolean(editingCredential)} />

        <Section flush title="Settings">
          <Collapse accordion className="settings-collapse" defaultActiveKey={['storage']} expandIconPosition="start" items={panelItems} />
        </Section>
      </div>
    </ConsoleShell>
  );
}
