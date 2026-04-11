import { useMemo } from 'react';
import type { ReactNode } from 'react';
import {
  AppstoreOutlined,
  CloudServerOutlined,
  FolderOpenOutlined,
  GlobalOutlined,
  HistoryOutlined,
  KeyOutlined,
  LinkOutlined,
  LockOutlined,
  SettingOutlined,
  SyncOutlined,
} from '@ant-design/icons';
import { Button, Grid, Layout, Menu, Space, Tag, Typography } from 'antd';
import type { MenuProps } from 'antd';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../auth';
import { pageMeta } from '../constants';
import { GlobalSearch } from './GlobalSearch';
import { RouteProgressBar } from './RouteProgressBar';
import { ThemeModeButton } from './ThemeModeButton';

const { Header, Sider, Content } = Layout;
const { Text, Title } = Typography;

type ShellProps = {
  children: ReactNode;
  actions?: ReactNode;
  showHeaderSearch?: boolean;
  meta?: { title: string; note: string };
};

export function ConsoleShell({ children, actions, showHeaderSearch = true, meta: metaOverride }: ShellProps) {
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const screens = Grid.useBreakpoint();
  const meta = metaOverride ?? pageMeta[location.pathname] ?? pageMeta['/overview'];

  const handleLogout = async () => {
    await auth.logout();
    navigate('/login');
  };

  const desktopMenuItems = useMemo<MenuProps['items']>(
    () => [
      {
        type: 'group',
        label: 'Overview',
        children: [
          { key: '/overview', icon: <AppstoreOutlined />, label: 'Overview' },
        ],
      },
      {
        type: 'group',
        label: 'Storage',
        children: [
          { key: '/buckets', icon: <CloudServerOutlined />, label: 'Buckets' },
          { key: '/browser', icon: <FolderOpenOutlined />, label: 'Browser' },
          { key: '/links', icon: <LinkOutlined />, label: 'Share links' },
        ],
      },
      {
        type: 'group',
        label: 'Configuration',
        children: [
          { key: '/access-keys', icon: <KeyOutlined />, label: 'Access keys' },
          { key: '/domains', icon: <GlobalOutlined />, label: 'Domains' },
          { key: '/sync', icon: <SyncOutlined />, label: 'Replication' },
        ],
      },
      {
        type: 'group',
        label: 'Operations',
        children: [
          { key: '/audit', icon: <HistoryOutlined />, label: 'Audit logs' },
          { key: '/settings', icon: <SettingOutlined />, label: 'Settings' },
        ],
      },
    ],
    [],
  );

  const mobileMenuItems = useMemo<MenuProps['items']>(
    () => [
      { key: '/overview', icon: <AppstoreOutlined />, label: 'Overview' },
      { key: '/buckets', icon: <CloudServerOutlined />, label: 'Buckets' },
      { key: '/browser', icon: <FolderOpenOutlined />, label: 'Browser' },
      { key: '/access-keys', icon: <KeyOutlined />, label: 'Access keys' },
      { key: '/audit', icon: <HistoryOutlined />, label: 'Audit logs' },
      { key: '/links', icon: <LinkOutlined />, label: 'Share links' },
      { key: '/domains', icon: <GlobalOutlined />, label: 'Domains' },
      { key: '/sync', icon: <SyncOutlined />, label: 'Replication' },
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
                <img alt="BareS3 logo" className="brand-mark" src="/logo.png" />
                <div>
                  <div className="brand-name">BareS3</div>
                  <div className="brand-note">file-first object storage</div>
                </div>
              </Link>

              <Menu
                className="shell-menu"
                items={desktopMenuItems}
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
                {showHeaderSearch ? <GlobalSearch /> : null}
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
                items={mobileMenuItems}
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
