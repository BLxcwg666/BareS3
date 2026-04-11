import { useMemo, useState } from 'react';
import { App as AntApp, Button, Descriptions, Divider, Empty, Form, Input, Modal, Popconfirm, Progress, Select, Skeleton, Space, Switch, Tag, Typography } from 'antd';
import { CopyOutlined, DeleteOutlined, EditOutlined, PlusOutlined, SyncOutlined, DatabaseOutlined, DownloadOutlined, UploadOutlined, HddOutlined, ApiOutlined, DesktopOutlined, ClockCircleOutlined, ControlOutlined, InboxOutlined, WarningOutlined, HeartOutlined, CheckCircleOutlined, CloseCircleOutlined, KeyOutlined, LinkOutlined } from '@ant-design/icons';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { useSyncStream } from '../hooks/useSyncStream';
import { useReplicationTokens } from '../hooks/useReplicationTokens';
import { useRuntimeData } from '../hooks/useRuntimeData';
import { createReplicationRemote, createReplicationToken, deleteReplicationRemote, deleteReplicationToken, resolveSyncConflict, revokeReplicationToken, updateReplicationRemote, updateSyncSettings, type ReplicationRemote, type SyncAccessToken } from '../api';
import { copyText, formatBytes, formatCount, formatDateTime, formatRelativeTime, formatTransferRate, nodeSummaryToItems, normalizeApiError } from '../utils';

const { Text } = Typography;

type TokenFormValues = {
  label: string;
};

type RemoteFormValues = {
  display_name: string;
  endpoint: string;
  token: string;
  enabled: boolean;
  follow_changes: boolean;
  bootstrap_mode: 'full' | 'from_now';
};

export function SyncPage() {
  const { message } = AntApp.useApp();
  const { settings, remotes, loading, refresh, setSettings } = useSyncStream();
  const { runtime } = useRuntimeData();
  const { tokens, loading: tokensLoading, refresh: refreshTokens } = useReplicationTokens();
  const [addRemoteOpen, setAddRemoteOpen] = useState(false);
  const [editRemote, setEditRemote] = useState<ReplicationRemote | null>(null);
  const [saving, setSaving] = useState(false);
  const [creatingToken, setCreatingToken] = useState(false);
  const [addingRemote, setAddingRemote] = useState(false);
  const [updatingRemoteID, setUpdatingRemoteID] = useState<string | null>(null);
  const [revokingTokenID, setRevokingTokenID] = useState<string | null>(null);
  const [deletingTokenID, setDeletingTokenID] = useState<string | null>(null);
  const [deletingRemoteID, setDeletingRemoteID] = useState<string | null>(null);
  const [resolvingConflictKey, setResolvingConflictKey] = useState<string | null>(null);
  const [tokenForm] = Form.useForm<TokenFormValues>();
  const [remoteForm] = Form.useForm<RemoteFormValues>();

  const localNodeItems = useMemo(() => {
    if (!settings || !runtime) {
      return [];
    }
    return [
      { label: 'Version', value: runtime.version.version || 'dev' },
      { label: 'Poll interval', value: `${settings.poll_interval_seconds || 10}s` },
      { label: 'Admin', value: runtime.listen.admin || 'Not configured' },
      { label: 'S3', value: runtime.listen.s3 || 'Not configured' },
      { label: 'Objects endpoint', value: runtime.listen.file || 'Not configured' },
      { label: 'Local cursor', value: String(settings.leader_cursor ?? 0) },
      { label: 'Legacy cursor', value: String(settings.applied_cursor ?? 0) },
      { label: 'Remotes', value: String(remotes.length) },
      { label: 'Pull tokens', value: String(tokens.length) }
    ];
  }, [remotes.length, runtime, settings, tokens.length]);

  const handleToggleSync = async (enabled: boolean) => {
    if (!settings) {
      return;
    }
    setSaving(true);
    try {
      const next = await updateSyncSettings({ enabled });
      setSettings((current) => ({
        ...current,
        enabled: next.enabled,
        poll_interval_seconds: next.poll_interval_seconds,
        role: next.role,
        leader_url: next.leader_url,
        shared_secret: next.shared_secret,
      }));
      message.success(enabled ? 'Sync enabled' : 'Sync disabled');
      await refresh();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to update sync state'));
    } finally {
      setSaving(false);
    }
  };

  const handleCreateToken = async (values: TokenFormValues) => {
    setCreatingToken(true);
    try {
      const token = await createReplicationToken(values.label);
      tokenForm.resetFields();
      await refreshTokens();
      await copyText(token.token);
      message.success('Created pull token and copied it');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to create pull token'));
    } finally {
      setCreatingToken(false);
    }
  };

  const handleAddRemote = async (values: RemoteFormValues) => {
    setAddingRemote(true);
    try {
      await createReplicationRemote(values);
      remoteForm.resetFields();
      remoteForm.setFieldValue('enabled', true);
      remoteForm.setFieldValue('bootstrap_mode', 'full');
      remoteForm.setFieldValue('follow_changes', true);
      message.success('Added replication remote');
      setAddRemoteOpen(false);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to add replication remote'));
    } finally {
      setAddingRemote(false);
    }
  };

  const handleRevokeToken = async (item: SyncAccessToken) => {
    setRevokingTokenID(item.id);
    try {
      await revokeReplicationToken(item.id);
      await refreshTokens();
      message.success('Revoked pull token');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to revoke pull token'));
    } finally {
      setRevokingTokenID(null);
    }
  };

  const handleDeleteRemote = async (item: ReplicationRemote) => {
    setDeletingRemoteID(item.id);
    try {
      await deleteReplicationRemote(item.id);
      await refresh();
      message.success(`Removed ${item.display_name}`);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to remove replication remote'));
    } finally {
      setDeletingRemoteID(null);
    }
  };

  const handleDeleteToken = async (item: SyncAccessToken) => {
    setDeletingTokenID(item.id);
    try {
      await deleteReplicationToken(item.id);
      await refreshTokens();
      message.success('Deleted revoked pull token');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to delete pull token'));
    } finally {
      setDeletingTokenID(null);
    }
  };

  const handleToggleFollowChanges = async (item: ReplicationRemote, checked: boolean) => {
    try {
      await updateReplicationRemote(item.id, { follow_changes: checked });
      await refresh();
      message.success(checked ? `Following ${item.display_name}` : `Paused live follow for ${item.display_name}`);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to update remote settings'));
    }
  };

  const openEditRemoteModal = (item: ReplicationRemote) => {
    setEditRemote(item);
    remoteForm.setFieldsValue({
      display_name: item.display_name,
      endpoint: item.endpoint,
      token: '',
      enabled: item.enabled,
      follow_changes: item.follow_changes,
      bootstrap_mode: item.bootstrap_mode,
    });
  };

  const closeRemoteModal = () => {
    setAddRemoteOpen(false);
    setEditRemote(null);
    remoteForm.resetFields();
    remoteForm.setFieldValue('enabled', true);
    remoteForm.setFieldValue('bootstrap_mode', 'full');
    remoteForm.setFieldValue('follow_changes', true);
  };

  const handleUpdateRemote = async (values: RemoteFormValues) => {
    if (!editRemote) {
      return;
    }
    setUpdatingRemoteID(editRemote.id);
    try {
      const payload: Parameters<typeof updateReplicationRemote>[1] = {
        display_name: values.display_name,
        endpoint: values.endpoint,
        bootstrap_mode: values.bootstrap_mode,
        enabled: values.enabled,
        follow_changes: values.follow_changes,
      };
      if (values.token.trim()) {
        payload.token = values.token.trim();
      }
      await updateReplicationRemote(editRemote.id, payload);
      await refresh();
      message.success(values.enabled ? `Updated ${editRemote.display_name}` : `Temporarily disabled ${editRemote.display_name}`);
      closeRemoteModal();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to update remote settings'));
    } finally {
      setUpdatingRemoteID(null);
    }
  };

  const handleResolveConflict = async (bucket: string, key: string) => {
    setResolvingConflictKey(`${bucket}/${key}`);
    try {
      await resolveSyncConflict(bucket, key);
      await refresh();
      message.success(`Re-fetched ${key} from source`);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to resolve conflict'));
    } finally {
      setResolvingConflictKey(null);
    }
  };

  const remoteProgressPercent = (remote: ReplicationRemote) => {
    if (remote.bytes_total > 0) {
      return Math.max(0, Math.min(100, Math.round((remote.bytes_completed / remote.bytes_total) * 100)));
    }
    if (remote.objects_total > 0) {
      return Math.max(0, Math.min(100, Math.round((remote.objects_completed / remote.objects_total) * 100)));
    }
    return remote.status === 'idle' ? 100 : 0;
  };

  const remoteProgressLabel = (remote: ReplicationRemote) => {
    if (remote.objects_total > 0 || remote.bytes_total > 0) {
      return 'Current pass';
    }
    if (!settings?.enabled) {
      return 'Sync disabled';
    }
    if (!remote.enabled) {
      return 'Node disabled';
    }
    if (remote.status === 'syncing') {
      return 'Discovering remote state';
    }
    if (remote.status === 'idle') {
      return 'Up to date';
    }
    if (remote.status === 'error') {
      return 'Needs attention';
    }
    return 'Waiting for next pass';
  };

  const renderRemoteStatus = (remote: ReplicationRemote) => {
    if (!settings?.enabled) {
      return <Tag>Disabled</Tag>;
    }
    if (!remote.enabled) {
      return <Tag>Disabled</Tag>;
    }
    if (remote.connection_status === 'connecting') {
      return <Tag color="processing">Connecting</Tag>;
    }
    if (remote.connection_status === 'disconnected' && remote.follow_changes) {
      return <Tag color="warning">Offline</Tag>;
    }
    if (remote.status === 'error' || remote.last_error?.trim()) {
      return <Tag color="error">Error</Tag>;
    }
    if (remote.status === 'syncing') {
      return <Tag color="processing">Syncing</Tag>;
    }
    if (remote.last_sync_at || remote.status === 'idle') {
      return <Tag color="success">Healthy</Tag>;
    }
    return <Tag color="processing">Pending sync</Tag>;
  };

  const renderRemoteCard = (remote: ReplicationRemote) => {
    return (
      <article className="sync-node-card" key={remote.id}>
        <div className="sync-node-card-head">
          <div className="sync-node-card-titlewrap">
            <div className="sync-node-card-title">{remote.display_name}</div>
            <div className="sync-node-card-subtitle">{remote.id}</div>
          </div>
          {renderRemoteStatus(remote)}
        </div>

        <div className="sync-node-card-meta sync-node-meta-flow">
          <span><ApiOutlined /> {remote.endpoint}</span>
          <span><DesktopOutlined /> Cursor {remote.cursor}</span>
          <span>
            <ClockCircleOutlined />{' '}
            {remote.status === 'syncing' && remote.last_sync_started_at
              ? `Started ${formatRelativeTime(remote.last_sync_started_at)}`
              : remote.last_sync_at
                ? `Synced ${formatRelativeTime(remote.last_sync_at)}`
                : 'No sync yet'}
          </span>
          <span style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            {remote.connection_status === 'connected' && remote.last_heartbeat_at
              ? <><HeartOutlined /> Heartbeat {formatRelativeTime(remote.last_heartbeat_at)}</>
              : remote.connection_status === 'connecting'
                ? <><SyncOutlined spin /> Connecting stream</>
                : <><WarningOutlined /> Stream offline</>}
            <Tag bordered={false} color={remote.enabled ? 'green' : 'default'} style={{ margin: 0 }}>{remote.enabled ? 'Enabled' : 'Disabled'}</Tag>
            <Tag bordered={false} color="blue" style={{ margin: 0 }}>{remote.follow_changes ? 'Live follow' : 'Snapshot only'}</Tag>
            <Tag bordered={false} color="default" style={{ margin: 0 }}>{remote.bootstrap_mode === 'from_now' ? 'From current cursor' : 'Full import'}</Tag>
          </span>
          {remote.last_error?.trim() ? <span>{remote.last_error}</span> : null}
        </div>

        <div className="sync-node-card-metrics">
          <div className="sync-node-card-metric">
            <span className="sync-node-card-label"><DatabaseOutlined /> Objects</span>
            <strong>
              {remote.objects_total > 0
                ? `${formatCount(remote.objects_completed)} / ${formatCount(remote.objects_total)}`
                : formatCount(remote.sync_counts?.ready ?? 0)}
            </strong>
          </div>
          <div className="sync-node-card-metric">
            <span className="sync-node-card-label"><HddOutlined /> Data</span>
            <strong>
              {remote.bytes_total > 0
                ? `${formatBytes(remote.bytes_completed)} / ${formatBytes(remote.bytes_total)}`
                : formatBytes(remote.bytes_completed)}
            </strong>
          </div>
          <div className="sync-node-card-metric">
            <span className="sync-node-card-label"><DownloadOutlined /> Down</span>
            <strong>{formatTransferRate(remote.download_rate_bps)}</strong>
          </div>
          <div className="sync-node-card-metric">
            <span className="sync-node-card-label"><UploadOutlined /> Up</span>
            <strong>{formatTransferRate(remote.upload_rate_bps)}</strong>
          </div>
        </div>

        <div className="sync-node-card-meta sync-node-meta-flow">
          <span><strong className="sync-node-card-label">Source cursor</strong> {formatCount(remote.peer_cursor ?? 0)}</span>
          <span><strong className="sync-node-card-label">Source buckets</strong> {formatCount(remote.peer_bucket_count ?? 0)}</span>
          <span><strong className="sync-node-card-label">Source objects</strong> {formatCount(remote.peer_object_count ?? 0)}</span>
          <span><strong className="sync-node-card-label">Source data</strong> {formatBytes(remote.peer_used_bytes ?? 0)}</span>
        </div>

        <div className="sync-node-progress-wrap">
          <div className="sync-node-progress-head">
            <span className="sync-progress-label">{remoteProgressLabel(remote)}</span>
          </div>
          <Progress percent={remoteProgressPercent(remote)} showInfo={false} size="small" status={remote.status === 'error' ? 'exception' : remote.status === 'idle' ? 'success' : 'active'} strokeColor="var(--accent)" />
        </div>

        <div className="sync-node-card-meta sync-node-meta-flow">
          <span><CheckCircleOutlined style={{ color: 'var(--success)' }} /> <span className="sync-node-card-label">Ready</span> {formatCount(remote.sync_counts?.ready ?? 0)}</span>
          <span><ClockCircleOutlined style={{ color: 'var(--warning)' }} /> <span className="sync-node-card-label">Queued</span> {formatCount((remote.sync_counts?.pending ?? 0) + (remote.sync_counts?.downloading ?? 0) + (remote.sync_counts?.verifying ?? 0))}</span>
          <span><CloseCircleOutlined style={{ color: 'var(--error)' }} /> <span className="sync-node-card-label">Errors</span> {formatCount((remote.sync_counts?.error ?? 0) + (remote.sync_counts?.conflict ?? 0))}</span>
        </div>

        {remote.last_error?.trim() ? (
          <div className="sync-local-error surface-soft danger-soft">
            <strong>Latest remote error</strong>
            <div>{remote.last_error}</div>
          </div>
        ) : null}

        <div className="sync-node-card-actions">
          <Space align="center" size="small">
            <Text type="secondary">Follow changes</Text>
            <Switch checked={remote.follow_changes} disabled={!remote.enabled} onChange={(checked) => void handleToggleFollowChanges(remote, checked)} size="small" />
          </Space>
          <Space size="small" wrap>
            <Button icon={<EditOutlined />} onClick={() => openEditRemoteModal(remote)} size="small">
              Edit
            </Button>
            <Popconfirm okText="Remove" onConfirm={() => void handleDeleteRemote(remote)} title={`Remove ${remote.display_name}?`}>
              <Button danger icon={<DeleteOutlined />} loading={deletingRemoteID === remote.id} size="small">
                Remove
              </Button>
            </Popconfirm>
          </Space>
        </div>
      </article>
    );
  };

  return (
    <ConsoleShell
      actions={
        <Space align="center" split={<Divider type="vertical" />} wrap>
          <Space align="center" size="small">
            <Text type="secondary">Replication</Text>
            <Switch checked={settings?.enabled ?? false} loading={saving} onChange={(checked) => void handleToggleSync(checked)} />
          </Space>
          <Button icon={<PlusOutlined />} onClick={() => setAddRemoteOpen(true)} type="primary">
            Add remote
          </Button>
        </Space>
      }
      showHeaderSearch={false}
    >
      {loading ? (
        <div className="workspace-stack">
          <Section title="Replication">
            <Skeleton active paragraph={{ rows: 10 }} title={false} />
          </Section>
        </div>
      ) : !settings ? (
        <div className="workspace-stack">
          <Section title="Replication">
            <Empty description="Replication settings are unavailable" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          </Section>
        </div>
      ) : (
        <div className="workspace-grid workspace-grid-main sync-layout-grid">
          <Section title="Replication remotes" note="Each remote is one-way. Add the reverse link on the other node if you want master-master replication.">
            {loading ? (
              <Skeleton active paragraph={{ rows: 6 }} title={false} />
            ) : remotes.length === 0 ? (
              <Empty description="No remotes yet. Use Add remote to configure a pull source." image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <div className="sync-node-grid">{remotes.map(renderRemoteCard)}</div>
            )}
          </Section>

          <Section title="Current node" note="Local listeners, sync status, and pull queue health for this instance.">
            {runtime ? (
              <div className="sync-local-card">
                <div className="sync-local-inline-stats">
                  <div className="sync-local-inline-stat">
                    <span className="sync-node-card-label"><DownloadOutlined /> Downloads</span>
                    <strong>{formatCount(settings?.reconcile_counts?.downloading ?? 0)}</strong>
                  </div>
                  <div className="sync-local-inline-stat">
                    <span className="sync-node-card-label"><WarningOutlined /> Errors</span>
                    <strong>{formatCount(settings?.reconcile_counts?.error ?? 0)}</strong>
                  </div>
                  <div className="sync-local-inline-stat">
                    <span className="sync-node-card-label"><ControlOutlined /> Conflicts</span>
                    <strong>{formatCount(settings?.reconcile_counts?.conflict ?? 0)}</strong>
                  </div>
                </div>

                <Descriptions column={1} items={nodeSummaryToItems([{ label: 'Version', value: runtime.version.version || 'dev' }, { label: 'Poll interval', value: `${settings?.poll_interval_seconds || 10}s` }, { label: 'Admin', value: runtime.listen.admin || 'Not configured' }, { label: 'S3', value: runtime.listen.s3 || 'Not configured' }, { label: 'Objects endpoint', value: runtime.listen.file || 'Not configured' }, { label: 'Local cursor', value: String(settings?.leader_cursor ?? 0) }, { label: 'Legacy cursor', value: String(settings?.applied_cursor ?? 0) }, { label: 'Remotes', value: String(remotes.length) }, { label: 'Pull tokens', value: String(tokens.length) }])} size="small" />

                <div className="sync-local-inline-stats">
                  <div className="sync-local-inline-stat">
                    <span className="sync-node-card-label"><HddOutlined /> Used storage</span>
                    <strong>{formatBytes(runtime.storage.used_bytes)}</strong>
                  </div>
                  <div className="sync-local-inline-stat">
                    <span className="sync-node-card-label"><InboxOutlined /> Buckets</span>
                    <strong>{runtime.storage.bucket_count}</strong>
                  </div>
                  <div className="sync-local-inline-stat">
                    <span className="sync-node-card-label"><LinkOutlined /> Links</span>
                    <strong>{runtime.storage.active_link_count}</strong>
                  </div>
                </div>

                {settings.reconcile_summary?.last_error ? (
                  <div className="sync-local-error surface-soft danger-soft">
                    <strong>Latest replication error</strong>
                    <div>{settings.reconcile_summary.last_error}</div>
                  </div>
                ) : null}

                {settings.conflict_items && settings.conflict_items.length > 0 ? (
                  <div className="sync-local-error surface-soft danger-soft">
                    <strong>Recent conflicts</strong>
                    <div className="sync-conflict-list">
                      {settings.conflict_items.slice(0, 5).map((item) => (
                        <div className="sync-conflict-row" key={`${item.bucket}/${item.key}/${item.updated_at}`}>
                          <div>
                            <div className="sync-node-card-title">{item.key}</div>
                            <Text type="secondary">{item.bucket} · {item.baseline_node_id || item.source || 'Unknown source'} · {formatRelativeTime(item.updated_at)}</Text>
                          </div>
                          <Text type="danger">{item.last_error || 'Conflict detected'}</Text>
                          <Button loading={resolvingConflictKey === `${item.bucket}/${item.key}`} onClick={() => void handleResolveConflict(item.bucket, item.key)} size="small" type="primary">
                            Re-fetch from source
                          </Button>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}

              </div>
            ) : (
              <Empty description="Current node details are unavailable" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </Section>

          <Section className="section-span-full" title="Pull tokens" note="Create a long-lived token on this node, then paste it into another node when adding this instance as a remote source.">
            {tokensLoading ? (
              <Skeleton active paragraph={{ rows: 4 }} title={false} />
            ) : tokens.length === 0 ? (
              <Empty description="No pull tokens yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <div className="sync-invite-list">
                {tokens.map((item) => (
                  <div className="sync-invite-row" key={item.id}>
                    <div className="sync-invite-copy">
                      <div className="sync-token-display">
                        <KeyOutlined className="sync-token-icon" /> <span>{item.token}</span>
                      </div>
                      <Text className="sync-token-meta" type="secondary">{item.label?.trim() || 'Unlabeled token'} · Created {formatDateTime(item.created_at)}</Text>
                    </div>
                    <Space wrap>
                      <Tag color={item.status === 'active' ? 'green' : 'orange'}>{item.status}</Tag>
                      <Button icon={<CopyOutlined />} onClick={() => void copyText(item.token)} size="small">
                        Copy
                      </Button>
                      {item.status === 'active' ? (
                        <Popconfirm okText="Revoke" onConfirm={() => void handleRevokeToken(item)} title="Revoke this token?">
                          <Button danger loading={revokingTokenID === item.id} size="small">
                            Revoke
                          </Button>
                        </Popconfirm>
                      ) : (
                        <Popconfirm okText="Delete" onConfirm={() => void handleDeleteToken(item)} title="Delete this revoked token?">
                          <Button danger loading={deletingTokenID === item.id} size="small">
                            Delete
                          </Button>
                        </Popconfirm>
                      )}
                    </Space>
                  </div>
                ))}
              </div>
            )}
          </Section>
        </div>
      )}

      <Modal destroyOnHidden footer={null} onCancel={closeRemoteModal} open={addRemoteOpen || Boolean(editRemote)} title={editRemote ? `Edit ${editRemote.display_name}` : 'Add remote'} width={720}>
        <div className="sync-modal-stack">
          {!editRemote ? (
            <section className="sync-modal-section">
              <div className="sync-modal-head">
                <div>
                  <strong>Create pull token</strong>
                  <div className="sync-modal-note">Generate a read token on this node so another node can pull from it.</div>
                </div>
              </div>

              <Form<TokenFormValues> form={tokenForm} layout="vertical" onFinish={(values) => void handleCreateToken(values)}>
                <Form.Item label="Label" name="label">
                  <Input placeholder="Laptop B" />
                </Form.Item>
                <Space>
                  <Button htmlType="submit" icon={<PlusOutlined />} loading={creatingToken} type="primary">
                    Create token
                  </Button>
                </Space>
              </Form>
            </section>
          ) : null}

          <section className="sync-modal-section">
            <div className="sync-modal-head">
              <div>
                <strong>{editRemote ? 'Edit replication source' : 'Add replication source'}</strong>
                <div className="sync-modal-note">{editRemote ? 'Update this source connection, rotate its token, or temporarily disable it without removing the node.' : 'Paste another node&apos;s token and admin endpoint. This link only pulls in one direction.'}</div>
              </div>
            </div>

            <Form<RemoteFormValues> form={remoteForm} initialValues={{ enabled: true, bootstrap_mode: 'full', follow_changes: true }} layout="vertical" onFinish={(values) => void (editRemote ? handleUpdateRemote(values) : handleAddRemote(values))}>
              <Form.Item label="Display name" name="display_name" rules={[{ required: true, message: 'Display name is required' }]}> 
                <Input placeholder="Office NAS" />
              </Form.Item>
              <Form.Item label="Admin endpoint" name="endpoint" rules={[{ required: true, message: 'Admin endpoint is required' }]}> 
                <Input placeholder="http://10.0.0.2:19080" />
              </Form.Item>
              <Form.Item initialValue="full" label="Bootstrap mode" name="bootstrap_mode" rules={[{ required: true, message: 'Bootstrap mode is required' }]}> 
                <Select
                  options={[
                    { label: 'Full import', value: 'full' },
                    { label: 'From current cursor', value: 'from_now' },
                  ]}
                />
              </Form.Item>
              <Form.Item label="Enable node" name="enabled" valuePropName="checked">
                <Switch />
              </Form.Item>
              <Form.Item label="Follow remote changes" name="follow_changes" valuePropName="checked">
                <Switch />
              </Form.Item>
              <Form.Item label="Pull token" name="token" rules={editRemote ? undefined : [{ required: true, message: 'Pull token is required' }]}> 
                <Input.TextArea autoSize={{ minRows: 2, maxRows: 4 }} placeholder={editRemote ? 'Leave empty to keep the current token, or paste a new token to rotate it' : 'Paste the token generated on the source node'} />
              </Form.Item>

              <Space>
                <Button htmlType="submit" icon={editRemote ? <EditOutlined /> : <SyncOutlined />} loading={editRemote ? updatingRemoteID === editRemote.id : addingRemote} type="primary">
                  {editRemote ? 'Save changes' : 'Add remote'}
                </Button>
                <Button onClick={closeRemoteModal}>Close</Button>
              </Space>
            </Form>
          </section>
        </div>
      </Modal>

    </ConsoleShell>
  );
}
