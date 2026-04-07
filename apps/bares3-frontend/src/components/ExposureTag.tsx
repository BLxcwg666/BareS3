import { Tag } from 'antd';

export function ExposureTag({ value }: { value: string }) {
  const tone = value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  return <Tag className={`mode-tag mode-tag-${tone}`}>{value}</Tag>;
}
