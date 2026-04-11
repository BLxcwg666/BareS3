import { useState } from 'react';
import { Empty, Table } from 'antd';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { TableFooterPagination } from '../components/TableFooterPagination';
import { useAuditActivity } from '../hooks/useAuditActivity';
import { auditLogColumns } from '../tables';

const auditPageSizeOptions = [20, 50, 100];

export function AuditLogsPage() {
  const { items, loading } = useAuditActivity(100);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  const start = (page - 1) * pageSize;
  const pageItems = items.slice(start, start + pageSize);

  return (
    <ConsoleShell showHeaderSearch={false}>
      <div className="workspace-stack">
        <Section flush title="Recent events">
          <Table
            columns={auditLogColumns}
            dataSource={pageItems}
            loading={loading}
            locale={{ emptyText: <Empty description="No audit events yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
            pagination={false}
            rowKey={(row) => `${row.time}-${row.action}-${row.target ?? row.title}`}
            scroll={{ x: 980 }}
            size="small"
          />
          <TableFooterPagination
            current={page}
            onChange={(nextPage, nextSize) => {
              if (nextSize !== pageSize) {
                setPageSize(nextSize);
                setPage(1);
              } else {
                setPage(nextPage);
              }
            }}
            pageSize={pageSize}
            pageSizeOptions={auditPageSizeOptions}
            total={items.length}
          />
        </Section>
      </div>
    </ConsoleShell>
  );
}
