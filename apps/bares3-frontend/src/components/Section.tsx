import type { ReactNode } from 'react';
import { Typography } from 'antd';

const { Text, Title } = Typography;

export function Section({
  title,
  note,
  extra,
  children,
  flush = false,
  className,
}: {
  title: string;
  note?: string;
  extra?: ReactNode;
  children: ReactNode;
  flush?: boolean;
  className?: string;
}) {
  return (
    <section className={className ? `workspace-section ${className}` : 'workspace-section'}>
      <div className="section-head">
        <div>
          <Title className="section-title" level={5}>
            {title}
          </Title>
          {note ? <Text className="section-note">{note}</Text> : null}
        </div>
        {extra}
      </div>
      <div className={flush ? 'section-body section-body-flush' : 'section-body'}>{children}</div>
    </section>
  );
}
