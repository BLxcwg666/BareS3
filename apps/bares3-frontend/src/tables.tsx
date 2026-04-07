import { Progress, Typography } from 'antd';
import type { TableColumnsType } from 'antd';
import type { AuditEntry, BucketInfo, ObjectInfo } from './api';
import { ExposureTag } from './components/ExposureTag';
import type { BucketDisplayRow } from './types';
import { buildBucketDisplayRows, formatBytes, formatDateTime, safePathLabel } from './utils';

const { Text } = Typography;

export const objectColumns: TableColumnsType<ObjectInfo> = [
  {
    dataIndex: 'key',
    key: 'key',
    title: 'Name',
    ellipsis: true,
  },
  {
    dataIndex: 'content_type',
    key: 'content_type',
    title: 'Type',
    width: 150,
    render: (value: string) => value || 'application/octet-stream',
  },
  {
    dataIndex: 'size',
    key: 'size',
    title: 'Size',
    width: 100,
    render: (value: number) => formatBytes(value),
  },
  {
    dataIndex: 'cache_control',
    key: 'cache_control',
    title: 'Cache',
    ellipsis: true,
    render: (value?: string) => value || 'private',
  },
  {
    dataIndex: 'last_modified',
    key: 'last_modified',
    title: 'Updated',
    width: 160,
    render: (value: string) => formatDateTime(value),
  },
];

export const auditLogColumns: TableColumnsType<AuditEntry> = [
  {
    dataIndex: 'title',
    key: 'title',
    title: 'Event',
    render: (value: string, row) => (
      <div>
        <div className="row-title">{value}</div>
        <div className="row-note">{row.detail || row.action}</div>
      </div>
    ),
  },
  {
    dataIndex: 'actor',
    key: 'actor',
    title: 'Actor',
    width: 120,
    render: (value: string) => value || 'system',
  },
  {
    dataIndex: 'target',
    key: 'target',
    title: 'Target',
    ellipsis: true,
    render: (value?: string) => value || 'N/A',
  },
  {
    dataIndex: 'remote',
    key: 'remote',
    title: 'Remote',
    width: 150,
    render: (value?: string) => value || 'N/A',
  },
  {
    dataIndex: 'time',
    key: 'time',
    title: 'Time',
    width: 180,
    render: (value: string) => formatDateTime(value),
  },
];

export function bucketColumns(compact = false): TableColumnsType<BucketDisplayRow> {
  const columns: TableColumnsType<BucketDisplayRow> = [
    {
      dataIndex: 'name',
      key: 'name',
      title: 'Bucket',
      render: (value: string, row) => (
        <div>
          <div className="row-title">{value}</div>
          <div className="row-note">{row.purpose}</div>
        </div>
      ),
    },
    {
      dataIndex: 'mode',
      key: 'mode',
      title: 'Mode',
      render: (value: string) => <ExposureTag value={value} />,
      width: 120,
    },
    {
      dataIndex: 'objects',
      key: 'objects',
      title: 'Objects',
      width: 110,
    },
    {
      dataIndex: 'size',
      key: 'size',
      title: 'Stored',
      width: 120,
    },
    {
      dataIndex: 'fill',
      key: 'fill',
      title: 'Used',
      render: (_value: string, row) =>
        row.fillPercent === null ? (
          <Text type="secondary">N/A</Text>
        ) : (
          <div className="used-cell">
            <Progress percent={row.fillPercent} showInfo={false} size="small" strokeColor="#5c775f" />
            <Text type="secondary">{row.fill}</Text>
          </div>
        ),
      width: 120,
    },
  ];

  if (!compact) {
    columns.splice(1, 0, {
      dataIndex: 'root',
      key: 'root',
      title: 'Root',
      render: (value: string, row) => (
        <div>
          <div className="row-title row-title-small">{safePathLabel(value)}</div>
          <div className="row-note">{row.policy}</div>
        </div>
      ),
    });
  }

  return columns;
}

export function bucketDisplayRows(buckets: BucketInfo[]) {
  return buildBucketDisplayRows(buckets);
}
