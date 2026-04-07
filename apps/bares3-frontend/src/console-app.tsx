import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import type { ChangeEvent, ReactNode } from 'react';
import {
  AppstoreOutlined,
  CloudServerOutlined,
  DesktopOutlined,
  DownOutlined,
  FolderOpenOutlined,
  HistoryOutlined,
  LinkOutlined,
  LockOutlined,
  MoonOutlined,
  SearchOutlined,
  SettingOutlined,
  SunOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import {
  Alert,
  Breadcrumb,
  Button,
  ConfigProvider,
  Descriptions,
  Dropdown,
  Empty,
  Form,
  Grid,
  Input,
  InputNumber,
  Layout,
  List,
  Menu,
  message,
  Modal,
  Progress,
  Select,
  Skeleton,
  Space,
  Spin,
  Table,
  Tag,
  theme as antTheme,
  Typography,
} from 'antd';
import type { DescriptionsProps, MenuProps, TableColumnsType, ThemeConfig } from 'antd';
import {
  HashRouter,
  Link,
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
} from 'react-router-dom';
import {
  createBucket,
  getRuntime,
  listAuditEntries,
  listBuckets,
  listObjects,
  updateStorageLimit,
  uploadObject,
  type AuditEntry,
  type AuthSession,
  type BucketInfo,
  type ObjectInfo,
  type RuntimeInfo,
  ApiError,
} from './api';
import { AuthProvider, GuestRoute, ProtectedRoute, useAuth } from './auth';
import {
  bucketRows as placeholderBucketRows,
  linkRows,
  loginNotes,
  nodeSummary as placeholderNodeSummary,
  overviewMetrics as placeholderOverviewMetrics,
  settingGroups as placeholderSettingGroups,
  type LinkRow,
} from './console-data';

const { Header, Sider, Content } = Layout;
const { Text, Title } = Typography;

type MetricItem = {
  label: string;
  value: string;
  detail: string;
};

type BucketDisplayRow = {
  name: string;
  purpose: string;
  root: string;
  mode: string;
  size: string;
  objects: string;
  fill: string;
  fillPercent: number | null;
  policy: string;
};

type BucketRow = BucketInfo;
type ObjectRow = ObjectInfo;

type ThemeMode = 'auto' | 'light' | 'dark';
type ResolvedTheme = 'light' | 'dark';
type SizeUnit = 'MB' | 'GB' | 'TB';

type BucketCreateValues = {
	name: string;
	quotaValue?: number;
	quotaUnit: SizeUnit;
};

type StorageLimitValues = {
	maxValue?: number;
	maxUnit: SizeUnit;
};

type ActivityDisplayItem = {
	key: string;
	title: string;
	meta: string;
	time: string;
};

const themeStorageKey = 'bares3-theme-mode';

const ThemeModeContext = createContext<{
  themeMode: ThemeMode;
  resolvedTheme: ResolvedTheme;
  setThemeMode: (value: ThemeMode) => void;
} | null>(null);

const themeModeMeta: Record<ThemeMode, { buttonLabel: string; menuLabel: string; icon: ReactNode }> = {
  auto: {
    buttonLabel: 'Theme: Auto',
    menuLabel: 'Auto (default)',
    icon: <DesktopOutlined />,
  },
  light: {
    buttonLabel: 'Theme: Light',
    menuLabel: 'Light',
    icon: <SunOutlined />,
  },
  dark: {
    buttonLabel: 'Theme: Dark',
    menuLabel: 'Dark',
    icon: <MoonOutlined />,
  },
};

const sizeUnitOptions: Array<{ label: SizeUnit; value: SizeUnit; bytes: number }> = [
  { label: 'MB', value: 'MB', bytes: 1024 ** 2 },
  { label: 'GB', value: 'GB', bytes: 1024 ** 3 },
  { label: 'TB', value: 'TB', bytes: 1024 ** 4 },
];

function readStoredThemeMode(): ThemeMode {
  if (typeof window === 'undefined') {
    return 'auto';
  }

  const value = window.localStorage.getItem(themeStorageKey);
  return value === 'light' || value === 'dark' || value === 'auto' ? value : 'auto';
}

function readSystemTheme(): ResolvedTheme {
  if (typeof window === 'undefined') {
    return 'light';
  }

  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function createThemeConfig(mode: ResolvedTheme): ThemeConfig {
  const isDark = mode === 'dark';

  return {
    algorithm: isDark ? antTheme.darkAlgorithm : antTheme.defaultAlgorithm,
    token: {
      colorPrimary: isDark ? '#8ba486' : '#4f6b56',
      colorInfo: isDark ? '#8ba486' : '#4f6b56',
      colorSuccess: isDark ? '#8ba486' : '#4f6b56',
      colorBgBase: isDark ? '#15171a' : '#f3f0e9',
      colorBgLayout: isDark ? '#15171a' : '#f3f0e9',
      colorBgContainer: isDark ? '#1c1f23' : '#faf7f0',
      colorBorder: isDark ? '#343a42' : '#d8d3c8',
      colorBorderSecondary: isDark ? '#2a2f36' : '#e5dfd4',
      colorText: isDark ? '#edf1f5' : '#1f231f',
      colorTextSecondary: isDark ? '#a6aeb7' : '#64665d',
      borderRadius: 6,
      fontFamily: "'IBM Plex Sans', 'Segoe UI', sans-serif",
      fontSize: 14,
      controlHeight: 34,
      lineWidth: 1,
    },
    components: {
      Button: {
        fontWeight: 600,
        paddingInline: 12,
      },
      Input: {
        activeBorderColor: isDark ? '#8ba486' : '#4f6b56',
        hoverBorderColor: isDark ? '#71866f' : '#7f8e7e',
      },
      Layout: {
        headerBg: 'transparent',
        siderBg: isDark ? '#1b1e22' : '#f9f6ef',
        bodyBg: isDark ? '#15171a' : '#f3f0e9',
      },
      Menu: {
        itemBg: 'transparent',
        itemBorderRadius: 6,
        itemSelectedBg: isDark ? '#263028' : '#eef1e7',
        itemSelectedColor: isDark ? '#edf1f5' : '#1f231f',
        itemHoverColor: isDark ? '#edf1f5' : '#1f231f',
        iconSize: 15,
      },
      Table: {
        headerBg: isDark ? '#23282d' : '#f4f1ea',
        headerColor: isDark ? '#a6aeb7' : '#64665d',
        cellPaddingBlock: 10,
        cellPaddingInline: 10,
        borderColor: isDark ? '#31363d' : '#e1dbcf',
      },
      Tag: {
        defaultBg: isDark ? '#24292e' : '#f1ece1',
        defaultColor: isDark ? '#edf1f5' : '#34372f',
      },
    },
  };
}

const pageMeta: Record<string, { title: string; note: string }> = {
  '/overview': { title: 'Overview', note: 'Buckets, routes, and current disk state.' },
  '/buckets': { title: 'Buckets', note: 'Readable roots with clear exposure rules.' },
  '/browser': { title: 'Browser', note: 'Objects, metadata, and current path context.' },
  '/audit': { title: 'Audit Logs', note: 'Recent console actions and storage changes.' },
  '/links': { title: 'Share links', note: 'Public routes, aliases, and signed delivery.' },
  '/settings': { title: 'Settings', note: 'Defaults that make the storage layer predictable.' },
};

function ConsoleApp() {
  const [themeMode, setThemeMode] = useState<ThemeMode>(() => readStoredThemeMode());
  const [systemTheme, setSystemTheme] = useState<ResolvedTheme>(() => readSystemTheme());

  useEffect(() => {
    if (typeof window === 'undefined') {
      return undefined;
    }

    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const updateSystemTheme = () => {
      setSystemTheme(media.matches ? 'dark' : 'light');
    };

    updateSystemTheme();

    if (typeof media.addEventListener === 'function') {
      media.addEventListener('change', updateSystemTheme);
      return () => media.removeEventListener('change', updateSystemTheme);
    }

    media.addListener(updateSystemTheme);
    return () => media.removeListener(updateSystemTheme);
  }, []);

  const resolvedTheme = themeMode === 'auto' ? systemTheme : themeMode;

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    window.localStorage.setItem(themeStorageKey, themeMode);
    document.documentElement.dataset.theme = resolvedTheme;
    document.documentElement.style.colorScheme = resolvedTheme;
  }, [resolvedTheme, themeMode]);

  const themeConfig = useMemo(() => createThemeConfig(resolvedTheme), [resolvedTheme]);
  const themeContextValue = useMemo(
    () => ({ themeMode, resolvedTheme, setThemeMode }),
    [resolvedTheme, themeMode],
  );

  return (
    <ThemeModeContext.Provider value={themeContextValue}>
      <ConfigProvider theme={themeConfig}>
        <HashRouter>
          <AuthProvider>
            <Routes>
              <Route path="/" element={<Navigate replace to="/login" />} />
              <Route
                path="/login"
                element={
                  <GuestRoute>
                    <LoginPage />
                  </GuestRoute>
                }
              />
              <Route
                path="/overview"
                element={
                  <ProtectedRoute>
                    <OverviewPage />
                  </ProtectedRoute>
                }
              />
              <Route
                path="/buckets"
                element={
                  <ProtectedRoute>
                    <BucketsPage />
                  </ProtectedRoute>
                }
              />
              <Route
                path="/browser"
                element={
                  <ProtectedRoute>
                    <BrowserPage />
                  </ProtectedRoute>
                }
              />
              <Route
                path="/audit"
                element={
                  <ProtectedRoute>
                    <AuditLogsPage />
                  </ProtectedRoute>
                }
              />
              <Route
                path="/links"
                element={
                  <ProtectedRoute>
                    <LinksPage />
                  </ProtectedRoute>
                }
              />
              <Route
                path="/settings"
                element={
                  <ProtectedRoute>
                    <SettingsPage />
                  </ProtectedRoute>
                }
              />
              <Route path="*" element={<Navigate replace to="/login" />} />
            </Routes>
          </AuthProvider>
        </HashRouter>
      </ConfigProvider>
    </ThemeModeContext.Provider>
  );
}

function useThemeMode() {
  const value = useContext(ThemeModeContext);

  if (!value) {
    throw new Error('Theme mode context is unavailable.');
  }

  return value;
}

function ThemeModeButton() {
  const { themeMode, setThemeMode } = useThemeMode();
  const currentTheme = themeModeMeta[themeMode];

  const menuItems = useMemo<MenuProps['items']>(
    () =>
      (Object.keys(themeModeMeta) as ThemeMode[]).map((key) => ({
        key,
        icon: themeModeMeta[key].icon,
        label: themeModeMeta[key].menuLabel,
      })),
    [],
  );

  return (
    <Dropdown
      menu={{
        items: menuItems,
        onClick: ({ key }) => setThemeMode(key as ThemeMode),
        selectable: true,
        selectedKeys: [themeMode],
      }}
      placement="bottomRight"
      trigger={['click']}
    >
      <Button className="theme-button">
        <Space size={6}>
          {currentTheme.icon}
          <span>{currentTheme.buttonLabel}</span>
          <DownOutlined />
        </Space>
      </Button>
    </Dropdown>
  );
}

function useRuntimeData() {
  const auth = useAuth();
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setRuntime(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setRuntime(await getRuntime());
    } finally {
      setLoading(false);
    }
  }, [auth.status]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { runtime, loading, refresh };
}

function useBucketsData() {
  const auth = useAuth();
  const [items, setItems] = useState<BucketInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setItems([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setItems(await listBuckets());
    } finally {
      setLoading(false);
    }
  }, [auth.status]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, loading, refresh };
}

function useAuditActivity(limit = 8) {
  const auth = useAuth();
  const [items, setItems] = useState<AuditEntry[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setItems([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setItems(await listAuditEntries(limit));
    } finally {
      setLoading(false);
    }
  }, [auth.status, limit]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, loading, refresh };
}

function useBucketObjects(bucket: string | null) {
  const auth = useAuth();
  const [items, setItems] = useState<ObjectInfo[]>([]);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated' || !bucket) {
      setItems([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setItems(await listObjects(bucket));
    } finally {
      setLoading(false);
    }
  }, [auth.status, bucket]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, loading, refresh };
}

function formatDateTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleString([], {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatRelativeTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  const deltaMs = parsed.getTime() - Date.now();
  const minutes = Math.round(deltaMs / 60000);
  if (Math.abs(minutes) < 60) {
    return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(minutes, 'minute');
  }

  const hours = Math.round(deltaMs / 3600000);
  if (Math.abs(hours) < 24) {
    return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(hours, 'hour');
  }

  const days = Math.round(deltaMs / 86400000);
  if (Math.abs(days) < 7) {
    return new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' }).format(days, 'day');
  }

  return formatDateTime(value);
}

function formatBytes(bytes: number) {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return '0 B';
  }
  if (bytes < 1024) {
    return `${bytes} B`;
  }

  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let index = -1;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }

  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[index]}`;
}

function formatCount(value: number) {
  return new Intl.NumberFormat().format(value);
}

function quotaLabel(bytes: number) {
  return bytes > 0 ? formatBytes(bytes) : 'Unlimited';
}

function usagePercentLabel(usedBytes: number, quotaBytes: number) {
  if (quotaBytes <= 0) {
    return 'N/A';
  }

  const percent = (usedBytes / quotaBytes) * 100;
  return `${percent >= 10 ? percent.toFixed(0) : percent.toFixed(1)}%`;
}

function usagePercentValue(usedBytes: number, quotaBytes: number) {
  if (quotaBytes <= 0) {
    return null;
  }

  return Math.max(0, Math.min(100, Math.round((usedBytes / quotaBytes) * 100)));
}

function sizeInputToBytes(value: number | null | undefined, unit: SizeUnit) {
  if (!value || value <= 0) {
    return 0;
  }

  const selectedUnit = sizeUnitOptions.find((option) => option.value === unit) ?? sizeUnitOptions[1];
  return Math.round(value * selectedUnit.bytes);
}

function bytesToSizeInput(bytes: number): { value?: number; unit: SizeUnit } {
  if (!bytes || bytes <= 0) {
    return { unit: 'GB' };
  }

  for (let index = sizeUnitOptions.length - 1; index >= 0; index -= 1) {
    const option = sizeUnitOptions[index];
    const amount = bytes / option.bytes;
    if (amount >= 1) {
      return {
        value: Number(amount.toFixed(amount >= 10 ? 0 : 1)),
        unit: option.value,
      };
    }
  }

  return {
    value: Number((bytes / sizeUnitOptions[0].bytes).toFixed(1)),
    unit: 'MB',
  };
}

function safePathLabel(value: string) {
  return value || 'Not configured';
}

function normalizeApiError(error: unknown, fallback: string) {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
}

type ShellProps = {
  children: ReactNode;
  actions?: ReactNode;
  showHeaderSearch?: boolean;
};

function ConsoleShell({ children, actions, showHeaderSearch = true }: ShellProps) {
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const screens = Grid.useBreakpoint();
  const meta = pageMeta[location.pathname] ?? pageMeta['/overview'];

  const handleLogout = async () => {
    await auth.logout();
    navigate('/login');
  };

  const menuItems = useMemo<MenuProps['items']>(
    () => [
      { key: '/overview', icon: <AppstoreOutlined />, label: 'Overview' },
      { key: '/buckets', icon: <CloudServerOutlined />, label: 'Buckets' },
      { key: '/browser', icon: <FolderOpenOutlined />, label: 'Browser' },
      { key: '/audit', icon: <HistoryOutlined />, label: 'Audit logs' },
      { key: '/links', icon: <LinkOutlined />, label: 'Share links' },
      { key: '/settings', icon: <SettingOutlined />, label: 'Settings' },
    ],
    [],
  );

  return (
    <>
      <RouteProgressBar />
      <Layout className="console-layout">
        {screens.lg ? (
          <Sider className="shell-sider" width={228}>
            <div className="sider-inner">
              <Link className="brand-row" to="/overview">
                <span className="brand-mark">B</span>
                <div>
                  <div className="brand-name">BareS3</div>
                  <div className="brand-note">file-first object storage</div>
                </div>
              </Link>

              <Menu
                className="shell-menu"
                items={menuItems}
                mode="inline"
                onClick={({ key }) => navigate(String(key))}
                selectedKeys={[location.pathname]}
              />

              <div className="sider-foot">
                <div className="user-foot">
                  <Text type="secondary">Signed in as</Text>
                  <strong>{auth.session?.username ?? 'admin'}</strong>
                </div>
                <Tag className="soft-tag">Session</Tag>
              </div>
            </div>
          </Sider>
        ) : null}

        <Layout className="shell-main">
          <Header className="shell-header">
            <div className="header-row">
              <div className="header-meta">
                <Title className="page-title" level={4}>
                  {meta.title}
                </Title>
                <Text className="page-note">{meta.note}</Text>
              </div>

              <Space className="header-actions" size={8} wrap>
               {showHeaderSearch ? (
                  <Input
                    allowClear
                    className="header-search"
                    placeholder="Search bucket or key"
                    prefix={<SearchOutlined />}
                  />
                ) : null}
                {actions}
                <ThemeModeButton />
                <Button icon={<LockOutlined />} onClick={() => void handleLogout()}>
                  Sign out
                </Button>
              </Space>
            </div>

            {!screens.lg ? (
              <Menu
                className="mobile-menu"
                items={menuItems}
                mode="horizontal"
                onClick={({ key }) => navigate(String(key))}
                selectedKeys={[location.pathname]}
              />
            ) : null}
          </Header>

          <Content className="shell-content">
            <div className="route-fade" key={location.pathname}>
              {children}
            </div>
          </Content>
        </Layout>
      </Layout>
    </>
  );
}

function RouteProgressBar() {
  const location = useLocation();
  const [state, setState] = useState<'idle' | 'loading' | 'done'>('idle');

  useEffect(() => {
    setState('loading');

    const complete = window.setTimeout(() => {
      setState('done');
    }, 140);

    const reset = window.setTimeout(() => {
      setState('idle');
    }, 320);

    return () => {
      window.clearTimeout(complete);
      window.clearTimeout(reset);
    };
  }, [location.pathname]);

  return <div aria-hidden className={`route-progress route-progress-${state}`} />;
}

function Section({
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

function ExposureTag({ value }: { value: string }) {
  const tone = value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  return <Tag className={`mode-tag mode-tag-${tone}`}>{value}</Tag>;
}

function metricStrip(items: MetricItem[]) {
  return (
    <div className="metric-strip">
      {items.map((item) => (
        <div className="metric-item" key={item.label}>
          <div className="metric-label">{item.label}</div>
          <div className="metric-value">{item.value}</div>
          <div className="metric-note">{item.detail}</div>
        </div>
      ))}
    </div>
  );
}

function buildBucketDisplayRows(buckets: BucketInfo[]): BucketDisplayRow[] {
  if (buckets.length === 0) {
    return placeholderBucketRows.map((bucket) => ({
      name: bucket.key,
      purpose: bucket.purpose,
      root: bucket.root,
      mode: bucket.mode,
      size: bucket.size,
      objects: bucket.objects,
      fill: bucket.used ? `${bucket.used}%` : 'N/A',
      fillPercent: bucket.used ?? null,
      policy: bucket.policy,
    }));
  }

  return buckets.map((bucket) => ({
    name: bucket.name,
    purpose: bucket.quota_bytes > 0 ? `Limit ${formatBytes(bucket.quota_bytes)}` : 'Unlimited bucket quota',
    root: bucket.path,
    mode: bucket.quota_bytes > 0 ? 'Limited' : 'Unlimited',
    size: formatBytes(bucket.used_bytes),
    objects: formatCount(bucket.object_count),
    fill: usagePercentLabel(bucket.used_bytes, bucket.quota_bytes),
    fillPercent: usagePercentValue(bucket.used_bytes, bucket.quota_bytes),
    policy: bucket.metadata_layout
      ? `Metadata: ${bucket.metadata_layout} • Quota: ${quotaLabel(bucket.quota_bytes)}`
      : `Quota: ${quotaLabel(bucket.quota_bytes)}`,
  }));
}

function bucketColumns(compact = false): TableColumnsType<BucketDisplayRow> {
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

const objectColumns: TableColumnsType<ObjectRow> = [
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

const linkColumns: TableColumnsType<LinkRow> = [
  {
    dataIndex: 'route',
    key: 'route',
    title: 'Route',
    ellipsis: true,
  },
  {
    dataIndex: 'target',
    key: 'target',
    title: 'Target',
    ellipsis: true,
  },
  {
    dataIndex: 'mode',
    key: 'mode',
    title: 'Mode',
    render: (value: LinkRow['mode']) => <ExposureTag value={value} />,
    width: 110,
  },
  {
    dataIndex: 'visits',
    key: 'visits',
    title: 'Visits',
    width: 90,
  },
  {
    dataIndex: 'expiry',
    key: 'expiry',
    title: 'Expiry',
    width: 100,
  },
];

const auditLogColumns: TableColumnsType<AuditEntry> = [
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

function BucketCreateModal({
  open,
  onCancel,
  onCreated,
}: {
  open: boolean;
  onCancel: () => void;
  onCreated: () => Promise<void> | void;
}) {
  const [form] = Form.useForm<BucketCreateValues>();
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) {
      return;
    }

    form.setFieldsValue({
      name: '',
      quotaValue: undefined,
      quotaUnit: 'GB',
    });
  }, [form, open]);

  const handleSubmit = async () => {
    const values = await form.validateFields();
    setSubmitting(true);
    try {
      const bucket = await createBucket(values.name.trim(), sizeInputToBytes(values.quotaValue, values.quotaUnit));
      message.success(`Bucket ${bucket.name} created`);
      form.resetFields();
      onCancel();
      await onCreated();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to create bucket'));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      confirmLoading={submitting}
      okText="Create bucket"
      onCancel={() => {
        if (submitting) {
          return;
        }
        form.resetFields();
        onCancel();
      }}
      onOk={() => void handleSubmit()}
      open={open}
      title="New bucket"
    >
      <Form form={form} initialValues={{ quotaUnit: 'GB' }} layout="vertical">
        <Form.Item label="Bucket name" name="name" rules={[{ required: true, whitespace: true, message: 'Bucket name is required' }]}>
          <Input placeholder="gallery" />
        </Form.Item>

        <Form.Item extra="Leave empty or 0 for unlimited." label="Bucket limit">
          <Space.Compact block>
            <Form.Item name="quotaValue" noStyle>
              <InputNumber min={0} placeholder="Unlimited" precision={1} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="quotaUnit" noStyle>
              <Select
                options={sizeUnitOptions.map((option) => ({ label: option.label, value: option.value }))}
                style={{ width: 96 }}
              />
            </Form.Item>
          </Space.Compact>
        </Form.Item>
      </Form>
    </Modal>
  );
}

function StorageLimitModal({
  currentMaxBytes,
  open,
  onCancel,
  onSaved,
}: {
  currentMaxBytes: number;
  open: boolean;
  onCancel: () => void;
  onSaved: () => Promise<void> | void;
}) {
  const [form] = Form.useForm<StorageLimitValues>();
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) {
      return;
    }

    const nextValue = bytesToSizeInput(currentMaxBytes);
    form.setFieldsValue({
      maxValue: nextValue.value,
      maxUnit: nextValue.unit,
    });
  }, [currentMaxBytes, form, open]);

  const handleSubmit = async () => {
    const values = await form.validateFields();
    setSubmitting(true);
    try {
      await updateStorageLimit(sizeInputToBytes(values.maxValue, values.maxUnit));
      message.success('Storage limit updated');
      onCancel();
      await onSaved();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to update storage limit'));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      confirmLoading={submitting}
      okText="Save limit"
      onCancel={() => {
        if (submitting) {
          return;
        }
        onCancel();
      }}
      onOk={() => void handleSubmit()}
      open={open}
      title="Instance storage limit"
    >
      <Form form={form} initialValues={{ maxUnit: 'GB' }} layout="vertical">
        <Form.Item extra="Leave empty or 0 for unlimited." label="Maximum storage">
          <Space.Compact block>
            <Form.Item name="maxValue" noStyle>
              <InputNumber min={0} placeholder="Unlimited" precision={1} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="maxUnit" noStyle>
              <Select
                options={sizeUnitOptions.map((option) => ({ label: option.label, value: option.value }))}
                style={{ width: 96 }}
              />
            </Form.Item>
          </Space.Compact>
        </Form.Item>
      </Form>
    </Modal>
  );
}

function LoginPage() {
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const redirectTarget = typeof location.state?.from === 'string' ? location.state.from : '/overview';

  const handleSubmit = async (values: { username: string; password: string }) => {
    setSubmitting(true);
    setError(null);
    try {
      await auth.login(values.username.trim(), values.password);
      navigate(redirectTarget, { replace: true });
    } catch (nextError) {
      setError(normalizeApiError(nextError, 'Failed to sign in.'));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="login-page">
      <div className="login-frame route-fade">
        <section className="login-aside">
          <div className="brand-row brand-row-login">
            <span className="brand-mark">B</span>
            <div>
              <div className="brand-name">BareS3</div>
              <div className="brand-note">file-first object storage</div>
            </div>
          </div>

          <div className="login-copy-block">
            <Title className="login-title" level={2}>
              Keep S3 outside. Keep files readable inside.
            </Title>
            <Text className="login-note-text">
              Sign in with the console account configured on this node, then manage buckets and file routes in one place.
            </Text>
          </div>

          <div className="login-lines">
            {loginNotes.map((item) => (
              <div className="login-line" key={item.label}>
                <span>{item.label}</span>
                <strong>{item.value}</strong>
              </div>
            ))}
          </div>
        </section>

        <section className="login-panel">
          <div className="login-panel-head">
            <div>
              <Title className="section-title" level={5}>
                Sign in
              </Title>
              <Text type="secondary">The console uses one configured admin account and a signed session cookie.</Text>
            </div>
            <ThemeModeButton />
          </div>

          <Form
            className="login-form"
            initialValues={{ username: 'admin', password: '' }}
            layout="vertical"
            onFinish={handleSubmit}
          >
            <Form.Item label="Username" name="username" rules={[{ required: true, message: 'Username is required' }]}>
              <Input autoComplete="username" />
            </Form.Item>
            <Form.Item label="Password" name="password" rules={[{ required: true, message: 'Password is required' }]}>
              <Input.Password autoComplete="current-password" />
            </Form.Item>

            {error ? <Alert className="login-alert" message={error} type="error" showIcon /> : null}

            <Space className="login-actions" size={8} wrap>
              <Button htmlType="submit" loading={submitting} type="primary">
                Enter console
              </Button>
            </Space>
          </Form>
        </section>
      </div>
    </div>
  );
}

function OverviewPage() {
  const navigate = useNavigate();
  const { runtime, loading: runtimeLoading, refresh: refreshRuntime } = useRuntimeData();
  const { items: buckets, loading: bucketsLoading, refresh: refreshBuckets } = useBucketsData();
  const { items: auditEntries, loading: activityLoading, refresh: refreshActivity } = useAuditActivity();
  const [isBucketModalOpen, setIsBucketModalOpen] = useState(false);

  const metrics: MetricItem[] = placeholderOverviewMetrics.map((item) => ({ ...item }));
  metrics[0] = {
    ...metrics[0],
    value: String(runtime?.storage.bucket_count ?? buckets.length),
    detail: buckets.length > 0 ? `${buckets.length} bucket${buckets.length === 1 ? '' : 's'} currently configured` : 'No buckets created yet',
  };
  metrics[1] = {
    ...metrics[1],
    value:
      runtimeLoading || !runtime?.storage.max_bytes
        ? 'N/A'
        : usagePercentLabel(runtime.storage.used_bytes, runtime.storage.max_bytes),
    detail:
      runtime?.storage.max_bytes && runtime.storage.max_bytes > 0
        ? `${formatBytes(runtime.storage.used_bytes)} of ${formatBytes(runtime.storage.max_bytes)} allocated`
        : 'Set an instance limit in Settings',
  };
  metrics[2] = {
    ...metrics[2],
    value: runtimeLoading ? 'N/A' : String(runtime?.storage.active_link_count ?? 0),
    detail: 'Link analytics will appear when share management is connected',
  };

  const overviewBuckets = buildBucketDisplayRows(buckets);
  const activityItems: ActivityDisplayItem[] = auditEntries.map((entry) => ({
    key: `${entry.time}-${entry.action}-${entry.target ?? entry.title}`,
    title: entry.title,
    meta: [entry.detail, entry.actor ? `by ${entry.actor}` : '', entry.remote].filter(Boolean).join(' · '),
    time: formatRelativeTime(entry.time),
  }));

  const nodeItems = placeholderNodeSummary.map((item) => ({ ...item }));
  nodeItems[0] = { label: 'Console', value: runtime?.app.name ?? 'BareS3' };
  nodeItems[1] = { label: 'Endpoint', value: runtime?.storage.s3_base_url ?? 'N/A' };
  nodeItems[2] = { label: 'Region', value: runtime?.storage.region ?? 'N/A' };
  nodeItems[3] = { label: 'Write mode', value: 'temp file then atomic rename' };

  return (
    <ConsoleShell
      actions={
        <>
          <Button onClick={() => setIsBucketModalOpen(true)}>New bucket</Button>
          <Button icon={<UploadOutlined />} onClick={() => navigate('/browser')} type="primary">
            Upload
          </Button>
        </>
      }
    >
      <div className="workspace-stack">
        <Section flush title="At a glance">
          {metricStrip(metrics)}
        </Section>

        <BucketCreateModal
          onCancel={() => setIsBucketModalOpen(false)}
          onCreated={() => Promise.all([refreshBuckets(), refreshRuntime(), refreshActivity()]).then(() => undefined)}
          open={isBucketModalOpen}
        />

        <div className="workspace-grid workspace-grid-main">
          <Section
            flush
            title="Buckets"
            extra={
              <Button onClick={() => navigate('/buckets')} size="small" type="link">
                Open
              </Button>
            }
          >
            <Table
              columns={bucketColumns(true)}
              dataSource={overviewBuckets}
              loading={bucketsLoading}
              locale={{ emptyText: <Empty description="No buckets yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
              pagination={false}
              rowKey="name"
              scroll={{ x: 720 }}
              size="small"
            />
          </Section>

          <Section
            title="Activity"
            extra={
              <Button onClick={() => navigate('/audit')} size="small" type="link">
                Open
              </Button>
            }
          >
            {activityLoading ? (
              <Skeleton active paragraph={{ rows: 4 }} title={false} />
            ) : activityItems.length === 0 ? (
              <Empty description="No recent activity" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <List
                dataSource={activityItems}
                renderItem={(item: ActivityDisplayItem) => (
                  <List.Item key={item.key}>
                    <List.Item.Meta description={item.meta || undefined} title={item.title} />
                    <Text type="secondary">{item.time}</Text>
                  </List.Item>
                )}
              />
            )}
          </Section>
        </div>

        <Section title="Node">
          {runtimeLoading ? (
            <Skeleton active paragraph={{ rows: 4 }} title={false} />
          ) : (
            <Descriptions column={1} items={nodeSummaryToItems(nodeItems)} size="small" />
          )}
        </Section>
      </div>
    </ConsoleShell>
  );
}

function BucketsPage() {
  const { items, loading, refresh } = useBucketsData();
  const displayRows = buildBucketDisplayRows(items);
  const [isBucketModalOpen, setIsBucketModalOpen] = useState(false);

  return (
    <ConsoleShell
      actions={
        <Button onClick={() => setIsBucketModalOpen(true)} type="primary">
          Create bucket
        </Button>
      }
    >
      <div className="workspace-stack">
        <BucketCreateModal
          onCancel={() => setIsBucketModalOpen(false)}
          onCreated={() => refresh()}
          open={isBucketModalOpen}
        />

        <Section flush title="All buckets">
          <Table
            columns={bucketColumns(false)}
            dataSource={displayRows}
            loading={loading}
            locale={{ emptyText: <Empty description="No buckets yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
            pagination={false}
            rowKey="name"
            scroll={{ x: 980 }}
            size="small"
          />
        </Section>
      </div>
    </ConsoleShell>
  );
}

function BrowserPage() {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const { items: buckets, loading: bucketsLoading } = useBucketsData();
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [searchValue, setSearchValue] = useState('');
  const [uploading, setUploading] = useState(false);
  const { items: objects, loading: objectsLoading, refresh } = useBucketObjects(selectedBucket);

  useEffect(() => {
    if (!selectedBucket && buckets.length > 0) {
      setSelectedBucket(buckets[0].name);
    }
    if (selectedBucket && !buckets.some((item) => item.name === selectedBucket)) {
      setSelectedBucket(buckets[0]?.name ?? null);
    }
  }, [buckets, selectedBucket]);

  const filteredObjects = useMemo(() => {
    const keyword = searchValue.trim().toLowerCase();
    if (!keyword) {
      return objects;
    }

    return objects.filter((item) =>
      [item.key, item.content_type, item.cache_control ?? '', item.etag ?? ''].some((field) =>
        field.toLowerCase().includes(keyword),
      ),
    );
  }, [objects, searchValue]);

  useEffect(() => {
    if (!selectedKey || !filteredObjects.some((item) => item.key === selectedKey)) {
      setSelectedKey(filteredObjects[0]?.key ?? null);
    }
  }, [filteredObjects, selectedKey]);

  const selectedObject = useMemo(
    () => filteredObjects.find((item) => item.key === selectedKey) ?? filteredObjects[0] ?? null,
    [filteredObjects, selectedKey],
  );

  const handleFileUpload = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!selectedBucket) {
      return;
    }
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    setUploading(true);
    try {
      const uploaded = await uploadObject(selectedBucket, file);
      message.success(`Uploaded ${uploaded.key}`);
      await refresh();
      setSelectedKey(uploaded.key);
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to upload object'));
    } finally {
      setUploading(false);
      event.target.value = '';
    }
  };

  const breadcrumbItems = selectedObject
    ? selectedObject.key.split('/').map((item) => ({ title: item }))
    : selectedBucket
      ? [{ title: selectedBucket }]
      : [{ title: 'No bucket selected' }];

  return (
    <ConsoleShell
      showHeaderSearch={false}
      actions={
        <>
          <input
            hidden
            onChange={handleFileUpload}
            ref={fileInputRef}
            type="file"
          />
          <Button
            disabled={!selectedBucket}
            icon={<UploadOutlined />}
            loading={uploading}
            onClick={() => fileInputRef.current?.click()}
            type="primary"
          >
            Add object
          </Button>
        </>
      }
    >
      <div className="workspace-stack">
        <div className="path-strip">
          <Breadcrumb items={breadcrumbItems} />
          <Select
            className="bucket-select"
            loading={bucketsLoading}
            onChange={(value) => {
              setSelectedBucket(value);
              setSelectedKey(null);
            }}
            options={buckets.map((bucket) => ({ label: bucket.name, value: bucket.name }))}
            placeholder="Select bucket"
            value={selectedBucket ?? undefined}
          />
        </div>

        <div className="workspace-grid workspace-grid-main">
          <Section
            flush
            title="Objects"
            extra={
              <Input
                allowClear
                className="section-search"
                onChange={(event) => setSearchValue(event.target.value)}
                placeholder="Search current path"
                prefix={<SearchOutlined />}
                value={searchValue}
              />
            }
          >
            <Table
              columns={objectColumns}
              dataSource={filteredObjects}
              loading={objectsLoading}
              locale={{
                emptyText: selectedBucket ? (
                  <Empty description="No objects in this bucket yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                ) : (
                  <Empty description="Create a bucket first" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                ),
              }}
              onRow={(record) => ({
                onClick: () => setSelectedKey(record.key),
              })}
              pagination={false}
              rowClassName={(record) => (record.key === selectedObject?.key ? 'table-row-selected' : '')}
              rowKey="key"
              scroll={{ x: 880 }}
              size="small"
            />
          </Section>

          <Section title="Inspector">
            {selectedObject ? (
              <Descriptions
                column={1}
                items={nodeSummaryToItems([
                  { label: 'Key', value: selectedObject.key },
                  { label: 'Content-Type', value: selectedObject.content_type || 'application/octet-stream' },
                  { label: 'Size', value: formatBytes(selectedObject.size) },
                  { label: 'Cache-Control', value: selectedObject.cache_control || 'private' },
                  { label: 'ETag', value: selectedObject.etag || 'Not set' },
                  { label: 'Updated', value: formatDateTime(selectedObject.last_modified) },
                ])}
                size="small"
              />
            ) : objectsLoading ? (
              <Spin />
            ) : (
              <Empty description="Select an object to inspect" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </Section>
        </div>
      </div>
    </ConsoleShell>
  );
}

function LinksPage() {
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

function AuditLogsPage() {
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

function SettingsPage() {
  const { runtime, loading, refresh } = useRuntimeData();
  const [isStorageModalOpen, setIsStorageModalOpen] = useState(false);

  const groups = placeholderSettingGroups.map((group) => ({
    title: group.title,
    items: group.items.map((item) => ({ ...item })),
  }));

  if (runtime) {
    groups[0] = {
      title: 'Endpoint identity',
      items: [
        { label: 'Console name', value: runtime.app.name },
        { label: 'S3 endpoint', value: runtime.storage.s3_base_url },
        { label: 'Region label', value: runtime.storage.region },
      ],
    };

    groups[1] = {
      title: 'Storage defaults',
      items: [
        { label: 'Bucket mapping', value: 'One bucket = one top-level folder' },
        { label: 'Metadata mode', value: runtime.storage.metadata_layout },
        { label: 'Upload safety', value: 'Temp write then atomic rename' },
      ],
    };

    groups[2] = {
      title: 'Delivery rules',
      items: [
        { label: 'Range requests', value: 'Enabled' },
        { label: 'Public links', value: 'Not wired yet' },
        { label: 'Default cache', value: 'Private unless published' },
      ],
    };
  }

  const maxBytes = runtime?.storage.max_bytes ?? 0;
  const usedBytes = runtime?.storage.used_bytes ?? 0;
  const remainingValue =
    maxBytes > 0
      ? usedBytes > maxBytes
        ? `Over by ${formatBytes(usedBytes - maxBytes)}`
        : formatBytes(maxBytes - usedBytes)
      : 'Unlimited';

  const capacityItems = [
    { label: 'Instance limit', value: quotaLabel(maxBytes) },
    { label: 'Used now', value: formatBytes(usedBytes) },
    { label: 'Remaining', value: remainingValue },
  ];

  return (
    <ConsoleShell>
      {loading ? (
        <div className="workspace-stack">
          <Section title="Runtime">
            <Skeleton active paragraph={{ rows: 8 }} title={false} />
          </Section>
        </div>
      ) : (
        <div className="workspace-stack">
          <StorageLimitModal
            currentMaxBytes={maxBytes}
            onCancel={() => setIsStorageModalOpen(false)}
            onSaved={() => refresh()}
            open={isStorageModalOpen}
          />

          <Section
            title="Capacity"
            note="Set the total space this BareS3 node is allowed to consume."
            extra={<Button onClick={() => setIsStorageModalOpen(true)}>Edit limit</Button>}
          >
            <Descriptions column={1} items={nodeSummaryToItems(capacityItems)} size="small" />
          </Section>

          <div className="workspace-grid workspace-grid-thirds">
            {groups.map((group) => (
              <Section key={group.title} title={group.title}>
                <Descriptions column={1} items={nodeSummaryToItems(group.items)} size="small" />
              </Section>
            ))}
          </div>
        </div>
      )}
    </ConsoleShell>
  );
}

function nodeSummaryToItems(
  items: Array<{ label: string; value: string }>,
): DescriptionsProps['items'] {
  return items.map((item) => ({
    key: item.label,
    label: item.label,
    children: item.value,
  }));
}

export default ConsoleApp;
