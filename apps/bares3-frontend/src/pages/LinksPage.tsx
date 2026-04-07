import { Button, Table } from 'antd';
import { linkRows } from '../console-data';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { linkColumns } from '../tables';

export function LinksPage() {
  return (
    <ConsoleShell
      actions={
        <Button disabled type="primary">
          Create link
        </Button>
      }
    >
      <div className="workspace-stack">
        <Section flush title="Published routes">
          <Table
            columns={linkColumns}
            dataSource={linkRows}
            pagination={false}
            rowKey="route"
            scroll={{ x: 860 }}
            size="small"
          />
        </Section>
      </div>
    </ConsoleShell>
  );
}
