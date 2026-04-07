import { useState } from 'react';
import { Button, Descriptions, Skeleton } from 'antd';
import { settingGroups as placeholderSettingGroups } from '../console-data';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { StorageLimitModal } from '../components/StorageLimitModal';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { formatBytes, nodeSummaryToItems, quotaLabel } from '../utils';

export function SettingsPage() {
  const { runtime, loading, refresh } = useRuntimeData();
  const [isStorageModalOpen, setIsStorageModalOpen] = useState(false);

  const groups = placeholderSettingGroups.map((group) => ({
    title: group.title,
    items: group.items.map((item) => ({ ...item })),
  }));

  if (runtime) {
    groups[0] = {
      title: 'Endpoint identity',
      items: [
        { label: 'Console name', value: runtime.app.name },
        { label: 'S3 endpoint', value: runtime.storage.s3_base_url },
        { label: 'Region label', value: runtime.storage.region },
      ],
    };

    groups[1] = {
      title: 'Storage defaults',
      items: [
        { label: 'Bucket mapping', value: 'One bucket = one top-level folder' },
        { label: 'Metadata mode', value: runtime.storage.metadata_layout },
        { label: 'Upload safety', value: 'Temp write then atomic rename' },
      ],
    };

    groups[2] = {
      title: 'Delivery rules',
      items: [
        { label: 'Range requests', value: 'Enabled' },
        { label: 'Public links', value: 'Not wired yet' },
        { label: 'Default cache', value: 'Private unless published' },
      ],
    };
  }

  const maxBytes = runtime?.storage.max_bytes ?? 0;
  const usedBytes = runtime?.storage.used_bytes ?? 0;
  const remainingValue =
    maxBytes > 0
      ? usedBytes > maxBytes
        ? `Over by ${formatBytes(usedBytes - maxBytes)}`
        : formatBytes(maxBytes - usedBytes)
      : 'Unlimited';

  const capacityItems = [
    { label: 'Instance limit', value: quotaLabel(maxBytes) },
    { label: 'Used now', value: formatBytes(usedBytes) },
    { label: 'Remaining', value: remainingValue },
  ];

  return (
    <ConsoleShell>
      {loading ? (
        <div className="workspace-stack">
          <Section title="Runtime">
            <Skeleton active paragraph={{ rows: 8 }} title={false} />
          </Section>
        </div>
      ) : (
        <div className="workspace-stack">
          <StorageLimitModal
            currentMaxBytes={maxBytes}
            onCancel={() => setIsStorageModalOpen(false)}
            onSaved={() => refresh()}
            open={isStorageModalOpen}
          />

          <Section
            title="Capacity"
            note="Set the total space this BareS3 node is allowed to consume."
            extra={<Button onClick={() => setIsStorageModalOpen(true)}>Edit limit</Button>}
          >
            <Descriptions column={1} items={nodeSummaryToItems(capacityItems)} size="small" />
          </Section>

          <div className="workspace-grid workspace-grid-thirds">
            {groups.map((group) => (
              <Section key={group.title} title={group.title}>
                <Descriptions column={1} items={nodeSummaryToItems(group.items)} size="small" />
              </Section>
            ))}
          </div>
        </div>
      )}
    </ConsoleShell>
  );
}
