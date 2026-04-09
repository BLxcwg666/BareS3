import { useMemo } from 'react';
import type { ReactNode } from 'react';
import {
  AppstoreOutlined,
  CloudServerOutlined,
  FolderOpenOutlined,
  HistoryOutlined,
  LinkOutlined,
  LockOutlined,
  SettingOutlined,
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
                <img alt="BareS3 logo" className="brand-mark" src="/logo.png" />
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
