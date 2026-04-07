import { useCallback, useEffect, useState } from 'react';
import { Button, Empty, Popconfirm, Space, Table, Tag, Typography, message } from 'antd';
import type { TableColumnsType } from 'antd';
import { listShareLinks, removeShareLink, revokeShareLink, type ShareLinkInfo, type ShareLinkStatus } from '../api';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { copyText, formatBytes, formatDateTime, formatRelativeTime, normalizeApiError } from '../utils';

const { Text } = Typography;

function statusTag(status: ShareLinkStatus) {
  switch (status) {
    case 'active':
      return <Tag color="success">Active</Tag>;
    case 'expired':
      return <Tag color="default">Expired</Tag>;
    case 'revoked':
      return <Tag color="warning">Revoked</Tag>;
    default:
      return <Tag>{status}</Tag>;
  }
}

export function LinksPage() {
  const [links, setLinks] = useState<ShareLinkInfo[]>([]);
  const [linksLoading, setLinksLoading] = useState(true);
  const [revokingId, setRevokingId] = useState<string | null>(null);
  const [removingId, setRemovingId] = useState<string | null>(null);

  const refreshLinks = useCallback(
    async (showError = true) => {
      setLinksLoading(true);
      try {
        setLinks(await listShareLinks());
      } catch (error) {
        if (showError) {
          message.error(normalizeApiError(error, 'Failed to load share links'));
        }
      } finally {
        setLinksLoading(false);
      }
    },
    [],
  );

  useEffect(() => {
    void refreshLinks(false);
  }, [refreshLinks]);

  const handleCopy = async (value: string, label: string) => {
    try {
      await copyText(value);
      message.success(`Copied ${label}`);
    } catch (error) {
      message.error(normalizeApiError(error, `Failed to copy ${label}`));
    }
  };

  const handleRevoke = async (link: ShareLinkInfo) => {
    setRevokingId(link.id);
    try {
      const revoked = await revokeShareLink(link.id);
      setLinks((current) => current.map((item) => (item.id === revoked.id ? revoked : item)));
      message.success('Revoked share link');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to revoke share link'));
    } finally {
      setRevokingId(null);
    }
  };

  const handleRemove = async (link: ShareLinkInfo) => {
    setRemovingId(link.id);
    try {
      await removeShareLink(link.id);
      setLinks((current) => current.filter((item) => item.id !== link.id));
      message.success('Removed share link');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to remove share link'));
    } finally {
      setRemovingId(null);
    }
  };

  const removeTitle = (link: ShareLinkInfo) =>
    `Permanently remove this ${link.status === 'expired' ? 'expired' : 'revoked'} link?`;

  const columns: TableColumnsType<ShareLinkInfo> = [
    {
      dataIndex: 'filename',
      key: 'filename',
      title: 'Object',
      render: (_value, row) => (
        <div>
          <div className="row-title">{row.filename || row.key}</div>
          <div className="row-note">{`${row.bucket}/${row.key}`}</div>
        </div>
      ),
    },
    {
      dataIndex: 'status',
      key: 'status',
      title: 'Status',
      width: 110,
      render: (value: ShareLinkStatus) => statusTag(value),
    },
    {
      dataIndex: 'created_at',
      key: 'created_at',
      title: 'Created',
      width: 170,
      render: (value: string, row) => (
        <div>
          <div className="row-title row-title-small">{formatDateTime(value)}</div>
          <div className="row-note">{row.created_by || 'system'}</div>
        </div>
      ),
    },
    {
      dataIndex: 'expires_at',
      key: 'expires_at',
      title: 'Expires',
      width: 180,
      render: (value: string, row) => (
        <div>
          <div className="row-title row-title-small">{formatDateTime(value)}</div>
          <div className="row-note">{row.status === 'revoked' ? 'Revoked' : formatRelativeTime(value)}</div>
        </div>
      ),
    },
    {
      dataIndex: 'size',
      key: 'size',
      title: 'Size',
      width: 110,
      render: (value: number) => formatBytes(value),
    },
    {
      key: 'access',
      title: 'Access',
      width: 220,
      render: (_value, row) => (
        <Space size={8} wrap>
          <Button onClick={() => void handleCopy(row.url, 'share URL')} size="small">
            Copy
          </Button>
          <Button href={row.url} rel="noreferrer" size="small" target="_blank">
            Open
          </Button>
          <Button href={row.download_url} rel="noreferrer" size="small" target="_blank">
            Download
          </Button>
        </Space>
      ),
    },
    {
      key: 'actions',
      title: 'Actions',
      width: 120,
      render: (_value, row) =>
        row.status === 'active' ? (
          <Popconfirm okText="Revoke" onConfirm={() => void handleRevoke(row)} title="Revoke this share link?">
            <Button danger loading={revokingId === row.id} size="small">
              Revoke
            </Button>
          </Popconfirm>
        ) : row.status === 'revoked' || row.status === 'expired' ? (
          <Popconfirm okText="Remove" onConfirm={() => void handleRemove(row)} title={removeTitle(row)}>
            <Button danger loading={removingId === row.id} size="small">
              Remove
            </Button>
          </Popconfirm>
        ) : (
          <Text type="secondary">-</Text>
        ),
    },
  ];

  return (
    <ConsoleShell showHeaderSearch={false}>
      <div className="workspace-stack">
        <Section
          flush
          title="Recent links"
          note="Create new links from the Browser inspector, then manage or revoke them here."
          extra={
            <Button loading={linksLoading} onClick={() => void refreshLinks()} size="small">
              Refresh
            </Button>
          }
        >
          <Table
            columns={columns}
            dataSource={links}
            loading={linksLoading}
            locale={{ emptyText: <Empty description="No share links yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
            pagination={{ pageSize: 10, showSizeChanger: false }}
            rowKey="id"
            scroll={{ x: 1080 }}
            size="small"
          />
        </Section>
      </div>
    </ConsoleShell>
  );
}
