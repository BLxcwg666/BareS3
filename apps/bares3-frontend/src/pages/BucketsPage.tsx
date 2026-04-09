import { useCallback, useMemo, useState } from 'react';
import { App as AntApp, Button, Empty, Popconfirm, Space, Table } from 'antd';
import type { TableColumnsType } from 'antd';
import { useNavigate } from 'react-router-dom';
import { deleteBucket } from '../api';
import { BucketCreateModal } from '../components/BucketCreateModal';
import { BucketEditDrawer } from '../components/BucketEditDrawer';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { useBucketsData } from '../hooks/useBucketsData';
import { bucketColumns, bucketDisplayRows } from '../tables';
import type { BucketDisplayRow } from '../types';
import { normalizeApiError } from '../utils';

export function BucketsPage() {
  const { message } = AntApp.useApp();
  const navigate = useNavigate();
  const { items, loading, refresh } = useBucketsData();
  const displayRows = bucketDisplayRows(items);
  const [isBucketModalOpen, setIsBucketModalOpen] = useState(false);
  const [editingBucketName, setEditingBucketName] = useState<string | null>(null);
  const [deletingBucket, setDeletingBucket] = useState<string | null>(null);
  const editingBucket = useMemo(() => items.find((item) => item.name === editingBucketName) ?? null, [editingBucketName, items]);

  const handleDeleteBucket = useCallback(
    async (name: string) => {
      setDeletingBucket(name);
      try {
        await deleteBucket(name);
        message.success(`Bucket ${name} deleted`);
        await refresh();
      } catch (error) {
        message.error(normalizeApiError(error, 'Failed to delete bucket'));
      } finally {
        setDeletingBucket(null);
      }
    },
    [refresh],
  );

  const columns = useMemo<TableColumnsType<BucketDisplayRow>>(
    () => [
      ...bucketColumns(false),
      {
        key: 'actions',
        title: 'Actions',
        width: 300,
        render: (_value, row) => (
          <Space size={8}>
            <Button onClick={() => setEditingBucketName(row.name)} size="small">
              Edit
            </Button>
            <Button onClick={() => navigate(`/buckets/${encodeURIComponent(row.name)}/access`)} size="small">
              Access
            </Button>
            <Button
              onClick={() => navigate({ pathname: '/browser', search: `?bucket=${encodeURIComponent(row.name)}` })}
              size="small"
            >
              Browse
            </Button>
            <Popconfirm
              cancelText="Cancel"
              okText="Delete"
              okButtonProps={{ danger: true, loading: deletingBucket === row.name }}
              onConfirm={() => void handleDeleteBucket(row.name)}
              title={`Delete bucket ${row.name}?`}
              description="This only works when the bucket is empty."
            >
              <Button danger loading={deletingBucket === row.name} size="small">
                Delete
              </Button>
            </Popconfirm>
          </Space>
        ),
      },
    ],
    [deletingBucket, handleDeleteBucket, navigate],
  );

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

        <BucketEditDrawer
          bucket={editingBucket}
          onCancel={() => setEditingBucketName(null)}
          onSaved={() => refresh()}
          open={editingBucket !== null}
        />

        <Section flush title="All buckets">
          <Table
            columns={columns}
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
