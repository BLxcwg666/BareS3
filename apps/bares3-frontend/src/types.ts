export type MetricItem = {
  label: string;
  value: string;
  detail: string;
};

export type BucketDisplayRow = {
  name: string;
  purpose: string;
  tags: string[];
  note: string;
  root: string;
  mode: string;
  size: string;
  objects: string;
  fill: string;
  fillPercent: number | null;
  policy: string;
};

export type ThemeMode = 'auto' | 'light' | 'dark';
export type ResolvedTheme = 'light' | 'dark';
export type SizeUnit = 'MB' | 'GB' | 'TB';

export type BucketCreateValues = {
  name: string;
  quotaValue?: number;
  quotaUnit: SizeUnit;
};

export type BucketEditValues = {
  name: string;
  quotaValue?: number;
  quotaUnit: SizeUnit;
  tags: string[];
  note: string;
};

export type StorageLimitValues = {
  maxValue?: number;
  maxUnit: SizeUnit;
};

export type ActivityDisplayItem = {
  key: string;
  title: string;
  meta: string;
  time: string;
};
