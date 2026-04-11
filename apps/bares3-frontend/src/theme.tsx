import { createContext, useContext, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { App as AntApp, ConfigProvider, theme as antTheme } from 'antd';
import type { ThemeConfig } from 'antd';
import { DesktopOutlined, MoonOutlined, SunOutlined } from '@ant-design/icons';
import { themeStorageKey } from './constants';
import type { ResolvedTheme, ThemeMode } from './types';

type ThemeModeContextValue = {
  themeMode: ThemeMode;
  resolvedTheme: ResolvedTheme;
  setThemeMode: (value: ThemeMode) => void;
};

const ThemeModeContext = createContext<ThemeModeContextValue | null>(null);

export const themeModeMeta: Record<ThemeMode, { buttonLabel: string; menuLabel: string; icon: ReactNode }> = {
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
      colorBgElevated: isDark ? '#22262b' : '#ffffff',
      colorBgSpotlight: isDark ? '#2a2f36' : '#ffffff',
      colorFillTertiary: isDark ? '#ffffff0f' : '#0000000a',
      colorFillQuaternary: isDark ? '#ffffff07' : '#00000005',
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
        popupBg: isDark ? '#22262b' : '#fdfbf7',
        itemHoverBg: isDark ? '#2a2f36' : '#f3eee4',
        itemSelectedBg: isDark ? '#263028' : '#dde6d7',
        itemSelectedColor: isDark ? '#edf1f5' : '#27402d',
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
      Modal: {
        contentBg: isDark ? '#22262b' : '#ffffff',
        headerBg: isDark ? '#22262b' : '#ffffff',
        footerBg: isDark ? '#22262b' : '#ffffff',
      },
      Drawer: {
        colorBgElevated: isDark ? '#22262b' : '#ffffff',
      },
      Select: {
        optionSelectedBg: isDark ? '#2a3028' : '#eef1e7',
      },
      Popover: {
        colorBgElevated: isDark ? '#22262b' : '#ffffff',
      },
    },
  };
}

export function ThemeProvider({ children }: { children: ReactNode }) {
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
        <AntApp>{children}</AntApp>
      </ConfigProvider>
    </ThemeModeContext.Provider>
  );
}

export function useThemeMode() {
  const value = useContext(ThemeModeContext);

  if (!value) {
    throw new Error('Theme mode context is unavailable.');
  }

  return value;
}
