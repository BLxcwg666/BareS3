import { useMemo } from 'react';
import { Button, Dropdown, Space } from 'antd';
import type { MenuProps } from 'antd';
import { DownOutlined } from '@ant-design/icons';
import { themeModeMeta, useThemeMode } from '../theme';
import type { ThemeMode } from '../types';

export function ThemeModeButton() {
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
      overlayClassName="theme-mode-dropdown"
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
