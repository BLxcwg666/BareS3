import { createContext, useContext, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import {
  AppstoreOutlined,
  CloudServerOutlined,
  DesktopOutlined,
  DownOutlined,
  FolderOpenOutlined,
  LinkOutlined,
  LockOutlined,
  MoonOutlined,
  SearchOutlined,
  SettingOutlined,
  SunOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import {
  Breadcrumb,
  Button,
  Checkbox,
  ConfigProvider,
  Descriptions,
  Dropdown,
  Form,
  Grid,
  Input,
  Layout,
  List,
  Menu,
  Progress,
  Space,
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
  activityRows,
  browserInspector,
  bucketRows,
  bucketTemplates,
  consoleRules,
  linkRows,
  loginNotes,
  nodeSummary,
  objectRows,
  overviewMetrics,
  publishingNotes,
  settingGroups,
  type BucketRow,
  type LinkRow,
  type ObjectRow,
} from './console-data';

const { Header, Sider, Content } = Layout;
const { Text, Title } = Typography;

type ThemeMode = 'auto' | 'light' | 'dark';
type ResolvedTheme = 'light' | 'dark';

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
          <Routes>
            <Route path="/" element={<Navigate replace to="/login" />} />
            <Route path="/login" element={<LoginPage />} />
            <Route path="/overview" element={<OverviewPage />} />
            <Route path="/buckets" element={<BucketsPage />} />
            <Route path="/browser" element={<BrowserPage />} />
            <Route path="/links" element={<LinksPage />} />
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="*" element={<Navigate replace to="/login" />} />
          </Routes>
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

type ShellProps = {
  children: ReactNode;
  actions?: ReactNode;
  showHeaderSearch?: boolean;
};

function ConsoleShell({ children, actions, showHeaderSearch = true }: ShellProps) {
  const location = useLocation();
  const navigate = useNavigate();
  const screens = Grid.useBreakpoint();
  const meta = pageMeta[location.pathname] ?? pageMeta['/overview'];

  const menuItems = useMemo<MenuProps['items']>(
    () => [
      { key: '/overview', icon: <AppstoreOutlined />, label: 'Overview' },
      { key: '/buckets', icon: <CloudServerOutlined />, label: 'Buckets' },
      { key: '/browser', icon: <FolderOpenOutlined />, label: 'Browser' },
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
                <Text type="secondary">Local backend online</Text>
                <Tag className="soft-tag">61.4% used</Tag>
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
                <Button icon={<LockOutlined />} onClick={() => navigate('/login')}>
                  Lock
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
}: {
  title: string;
  note?: string;
  extra?: ReactNode;
  children: ReactNode;
  flush?: boolean;
}) {
  return (
    <section className="workspace-section">
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
  const tone = value.toLowerCase().replace(/\s+/g, '-');
  return <Tag className={`mode-tag mode-tag-${tone}`}>{value}</Tag>;
}

function metricStrip() {
  return (
    <div className="metric-strip">
      {overviewMetrics.map((item) => (
        <div className="metric-item" key={item.label}>
          <div className="metric-label">{item.label}</div>
          <div className="metric-value">{item.value}</div>
          <div className="metric-note">{item.detail}</div>
        </div>
      ))}
    </div>
  );
}

function bucketColumns(compact = false): TableColumnsType<BucketRow> {
  const columns: TableColumnsType<BucketRow> = [
    {
      dataIndex: 'key',
      key: 'key',
      title: 'Bucket',
      render: (_, row) => (
        <div>
          <div className="row-title">{row.key}</div>
          <div className="row-note">{row.purpose}</div>
        </div>
      ),
    },
    {
      dataIndex: 'mode',
      key: 'mode',
      title: 'Mode',
      render: (value: BucketRow['mode']) => <ExposureTag value={value} />,
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
      dataIndex: 'used',
      key: 'used',
      title: 'Used',
      render: (value: number) => (
        <div className="used-cell">
          <Progress percent={value} showInfo={false} size="small" strokeColor="#5c775f" />
          <Text type="secondary">{value}%</Text>
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
          <div className="row-title row-title-small">{value}</div>
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
    dataIndex: 'type',
    key: 'type',
    title: 'Type',
    width: 140,
  },
  {
    dataIndex: 'size',
    key: 'size',
    title: 'Size',
    width: 92,
  },
  {
    dataIndex: 'cache',
    key: 'cache',
    title: 'Cache',
    ellipsis: true,
  },
  {
    dataIndex: 'updated',
    key: 'updated',
    title: 'Updated',
    width: 160,
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

function LoginPage() {
  const navigate = useNavigate();

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
              A practical console for image hosting, download shelves, and personal storage.
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
              <Text type="secondary">Static page for now, ready for the real flow later.</Text>
            </div>
            <ThemeModeButton />
          </div>

          <Form
            className="login-form"
            initialValues={{
              endpoint: 'https://s3.bare.local',
              accessKey: 'bares3-admin',
              secretKey: 'local-dev-only',
              remember: true,
              fileFirst: true,
            }}
            layout="vertical"
            onFinish={() => navigate('/overview')}
          >
            <Form.Item label="Endpoint" name="endpoint">
              <Input />
            </Form.Item>
            <Form.Item label="Access key" name="accessKey">
              <Input />
            </Form.Item>
            <Form.Item label="Secret key" name="secretKey">
              <Input.Password />
            </Form.Item>

            <Space className="login-checks" direction="vertical" size={6}>
              <Form.Item name="remember" noStyle valuePropName="checked">
                <Checkbox>Remember endpoint</Checkbox>
              </Form.Item>
              <Form.Item name="fileFirst" noStyle valuePropName="checked">
                <Checkbox>Prefer file-first defaults</Checkbox>
              </Form.Item>
            </Space>

            <Space className="login-actions" size={8} wrap>
              <Button htmlType="submit" type="primary">
                Enter console
              </Button>
              <Button onClick={() => navigate('/browser')}>Preview browser</Button>
            </Space>
          </Form>
        </section>
      </div>
    </div>
  );
}

function OverviewPage() {
  const navigate = useNavigate();

  return (
    <ConsoleShell
      actions={
        <>
          <Button>New bucket</Button>
          <Button icon={<UploadOutlined />} type="primary">
            Upload
          </Button>
        </>
      }
    >
      <div className="workspace-stack">
        <Section flush title="At a glance">
          {metricStrip()}
        </Section>

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
              dataSource={bucketRows}
              pagination={false}
              rowKey="key"
              scroll={{ x: 720 }}
              size="small"
            />
          </Section>

          <Section title="Activity">
            <List
              dataSource={activityRows}
              renderItem={(item) => (
                <List.Item>
                  <List.Item.Meta description={item.meta} title={item.title} />
                  <Text type="secondary">{item.time}</Text>
                </List.Item>
              )}
            />
          </Section>
        </div>

        <div className="workspace-grid workspace-grid-main">
          <Section title="Node">
            <Descriptions column={1} items={nodeSummaryToItems(nodeSummary)} size="small" />
          </Section>

          <Section title="Rules">
            <List
              dataSource={consoleRules}
              renderItem={(item) => (
                <List.Item>
                  <Text>{item}</Text>
                </List.Item>
              )}
            />
          </Section>
        </div>
      </div>
    </ConsoleShell>
  );
}

function BucketsPage() {
  return (
    <ConsoleShell
      actions={
        <Button type="primary">
          Create bucket
        </Button>
      }
    >
      <div className="workspace-stack">
        <div className="workspace-grid workspace-grid-main">
          <Section flush title="All buckets">
            <Table
              columns={bucketColumns(false)}
              dataSource={bucketRows}
              pagination={false}
              rowKey="key"
              scroll={{ x: 860 }}
              size="small"
            />
          </Section>

          <Section title="Templates">
            <List
              dataSource={bucketTemplates}
              renderItem={(item) => (
                <List.Item>
                  <Text>{item}</Text>
                </List.Item>
              )}
            />
          </Section>
        </div>
      </div>
    </ConsoleShell>
  );
}

function BrowserPage() {
  const [searchValue, setSearchValue] = useState('');

  const filteredObjects = useMemo(() => {
    const keyword = searchValue.trim().toLowerCase();

    if (!keyword) {
      return objectRows;
    }

    return objectRows.filter((item) =>
      [item.key, item.type, item.cache, item.etag].some((field) =>
        field.toLowerCase().includes(keyword),
      ),
    );
  }, [searchValue]);

  return (
    <ConsoleShell
      showHeaderSearch={false}
      actions={
        <Button icon={<UploadOutlined />} type="primary">
          Add object
        </Button>
      }
    >
      <div className="workspace-stack">
        <div className="path-strip">
          <Breadcrumb
            items={[
              { title: 'gallery' },
              { title: 'launch' },
              { title: 'mock-02.png' },
            ]}
          />
          <Tag className="soft-tag">range requests on</Tag>
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
              pagination={false}
              rowKey="key"
              scroll={{ x: 880 }}
              size="small"
            />
          </Section>

          <Section title="Inspector">
            <Descriptions column={1} items={nodeSummaryToItems(browserInspector)} size="small" />
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
        <Button type="primary">
          Create link
        </Button>
      }
    >
      <div className="workspace-stack">
        <div className="workspace-grid workspace-grid-main">
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

          <Section title="Publishing rules">
            <List
              dataSource={publishingNotes}
              renderItem={(item) => (
                <List.Item>
                  <Text>{item}</Text>
                </List.Item>
              )}
            />
          </Section>
        </div>
      </div>
    </ConsoleShell>
  );
}

function SettingsPage() {
  return (
    <ConsoleShell>
      <div className="workspace-grid workspace-grid-thirds">
        {settingGroups.map((group) => (
          <Section key={group.title} title={group.title}>
            <Descriptions column={1} items={nodeSummaryToItems(group.items)} size="small" />
          </Section>
        ))}
      </div>
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
