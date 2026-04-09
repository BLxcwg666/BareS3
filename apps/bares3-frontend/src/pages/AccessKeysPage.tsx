import { useMemo, useState } from 'react';
import { App as AntApp, Button, Empty, Popconfirm, Space, Table, Tag } from 'antd';
import type { TableColumnsType } from 'antd';
import { removeS3Credential, revokeS3Credential, type S3CredentialInfo } from '../api';
import { ConsoleShell } from '../components/ConsoleShell';
import { S3CredentialModal } from '../components/S3CredentialModal';
import { Section } from '../components/Section';
import { useBucketsData } from '../hooks/useBucketsData';
import { useS3CredentialsData } from '../hooks/useS3CredentialsData';
import { formatDateTime, normalizeApiError, s3CredentialBucketScopeLabel, s3CredentialPermissionLabel } from '../utils';

function credentialSourceLabel(value?: string) {
  return value === 'config' ? 'Imported from config' : 'Managed in console';
}

export function AccessKeysPage() {
  const { message } = AntApp.useApp();
  const { items: credentials, loading, refresh } = useS3CredentialsData();
  const { items: buckets } = useBucketsData();
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [editingCredential, setEditingCredential] = useState<S3CredentialInfo | null>(null);
  const [revokingAccessKeyID, setRevokingAccessKeyID] = useState<string | null>(null);
  const [removingAccessKeyID, setRemovingAccessKeyID] = useState<string | null>(null);

  const bucketNames = useMemo(() => buckets.map((bucket) => bucket.name), [buckets]);

  async function handleRevoke(accessKeyID: string) {
    setRevokingAccessKeyID(accessKeyID);
    try {
      await revokeS3Credential(accessKeyID);
      message.success(`Revoked ${accessKeyID}`);
      await refresh();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to revoke access key'));
    } finally {
      setRevokingAccessKeyID(null);
    }
  }

  async function handleRemove(accessKeyID: string) {
    setRemovingAccessKeyID(accessKeyID);
    try {
      await removeS3Credential(accessKeyID);
      message.success(`Deleted ${accessKeyID}`);
      await refresh();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to delete revoked access key'));
    } finally {
      setRemovingAccessKeyID(null);
    }
  }

  const columns = useMemo<TableColumnsType<S3CredentialInfo>>(
    () => [
      {
        dataIndex: 'access_key_id',
        key: 'access_key_id',
        title: 'Access key',
        render: (value: string, row) => (
          <div>
            <div className="row-title row-title-small">{value}</div>
            <div className="row-note">{row.label || credentialSourceLabel(row.source)}</div>
          </div>
        ),
      },
      {
        dataIndex: 'permission',
        key: 'permission',
        title: 'Permission',
        width: 130,
        render: (value: string) => s3CredentialPermissionLabel(value),
      },
      {
        dataIndex: 'buckets',
        key: 'buckets',
        title: 'Buckets',
        render: (value: string[]) => s3CredentialBucketScopeLabel(value),
      },
      {
        dataIndex: 'last_used_at',
        key: 'last_used_at',
        title: 'Last used',
        width: 180,
        render: (value?: string) => (value ? formatDateTime(value) : 'Never'),
      },
      {
        dataIndex: 'status',
        key: 'status',
        title: 'Status',
        width: 110,
        render: (value: string) => <Tag color={value === 'active' ? 'green' : 'default'}>{value === 'active' ? 'Active' : 'Revoked'}</Tag>,
      },
      {
        key: 'actions',
        title: 'Actions',
        width: 220,
        render: (_value, row) =>
          row.status === 'active' ? (
            <Space size={8} wrap>
              <Button onClick={() => setEditingCredential(row)} size="small">
                Edit
              </Button>
              <Popconfirm
                cancelText="Cancel"
                description="Clients using this key stop working immediately."
                okButtonProps={{ danger: true, loading: revokingAccessKeyID === row.access_key_id }}
                okText="Revoke"
                onConfirm={() => void handleRevoke(row.access_key_id)}
                title={`Revoke ${row.access_key_id}?`}
              >
                <Button danger loading={revokingAccessKeyID === row.access_key_id} size="small">
                  Revoke
                </Button>
              </Popconfirm>
            </Space>
          ) : (
            <Popconfirm
              cancelText="Cancel"
              description="This permanently removes the revoked key from the console."
              okButtonProps={{ danger: true, loading: removingAccessKeyID === row.access_key_id }}
              okText="Delete"
              onConfirm={() => void handleRemove(row.access_key_id)}
              title={`Delete ${row.access_key_id}?`}
            >
              <Button danger loading={removingAccessKeyID === row.access_key_id} size="small">
                Delete
              </Button>
            </Popconfirm>
          ),
      },
    ],
    [removingAccessKeyID, revokingAccessKeyID],
  );

  return (
    <ConsoleShell
      actions={
        <Space size={8} wrap>
          <Button onClick={() => void refresh()} size="small">
            Refresh
          </Button>
          <Button onClick={() => setIsCreateModalOpen(true)} type="primary">
            Create key
          </Button>
        </Space>
      }
    >
      <div className="workspace-stack">
        <S3CredentialModal bucketNames={bucketNames} credential={null} onCancel={() => setIsCreateModalOpen(false)} onSaved={() => refresh()} open={isCreateModalOpen} />
        <S3CredentialModal bucketNames={bucketNames} credential={editingCredential} onCancel={() => setEditingCredential(null)} onSaved={() => refresh()} open={Boolean(editingCredential)} />

        <Section
          flush
          title="Managed keys"
          note="Create S3 credentials with read-only or read-write scope, restrict them to selected buckets, and watch the last successful use time."
        >
          <Table
            columns={columns}
            dataSource={credentials}
            loading={loading}
            locale={{ emptyText: <Empty description="No access keys yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
            pagination={false}
            rowKey="access_key_id"
            scroll={{ x: 960 }}
            size="small"
          />
        </Section>
      </div>
    </ConsoleShell>
  );
}
