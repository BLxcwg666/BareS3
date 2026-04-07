import { Button, Empty, Table } from 'antd';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { useAuditActivity } from '../hooks/useAuditActivity';
import { auditLogColumns } from '../tables';

export function AuditLogsPage() {
  const { items, loading, refresh } = useAuditActivity(100);

  return (
    <ConsoleShell
      showHeaderSearch={false}
      actions={
        <Button onClick={() => void refresh()} type="primary">
          Refresh
        </Button>
      }
    >
      <div className="workspace-stack">
        <Section flush title="Recent events">
          <Table
            columns={auditLogColumns}
            dataSource={items}
            loading={loading}
            locale={{ emptyText: <Empty description="No audit events yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
            pagination={{ pageSize: 20, showSizeChanger: false }}
            rowKey={(row) => `${row.time}-${row.action}-${row.target ?? row.title}`}
            scroll={{ x: 980 }}
            size="small"
          />
        </Section>
      </div>
    </ConsoleShell>
  );
}
