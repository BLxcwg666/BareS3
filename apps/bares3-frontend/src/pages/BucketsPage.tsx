import { useState } from 'react';
import { Button, Empty, Table } from 'antd';
import { BucketCreateModal } from '../components/BucketCreateModal';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { useBucketsData } from '../hooks/useBucketsData';
import { bucketColumns, bucketDisplayRows } from '../tables';

export function BucketsPage() {
  const { items, loading, refresh } = useBucketsData();
  const displayRows = bucketDisplayRows(items);
  const [isBucketModalOpen, setIsBucketModalOpen] = useState(false);

  return (
    <ConsoleShell
      actions={
        <Button onClick={() => setIsBucketModalOpen(true)} type="primary">
          Create bucket
        </Button>
      }
    >
      <div className="workspace-stack">
        <BucketCreateModal
          onCancel={() => setIsBucketModalOpen(false)}
          onCreated={() => refresh()}
          open={isBucketModalOpen}
        />

        <Section flush title="All buckets">
          <Table
            columns={bucketColumns(false)}
            dataSource={displayRows}
            loading={loading}
            locale={{ emptyText: <Empty description="No buckets yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
            pagination={false}
            rowKey="name"
            scroll={{ x: 980 }}
            size="small"
          />
        </Section>
      </div>
    </ConsoleShell>
  );
}
