import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { Button, Spin, theme } from 'antd';
import { ApiError, getSession, login as loginRequest, logout as logoutRequest } from './api';
import type { AuthSession } from './api';

type AuthStatus = 'loading' | 'authenticated' | 'guest' | 'unavailable';

type AuthContextValue = {
  status: AuthStatus;
  session: AuthSession | null;
  error: string | null;
  refresh: () => Promise<void>;
  login: (username: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>('loading');
  const [session, setSession] = useState<AuthSession | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setStatus((current) => (current === 'authenticated' ? current : 'loading'));
    try {
      const nextSession = await getSession();
      setSession(nextSession);
      setError(null);
      setStatus('authenticated');
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        setSession(null);
        setError(null);
        setStatus('guest');
        return;
      }

      setError(error instanceof Error ? error.message : 'Unable to verify session.');
      setStatus((current) => (current === 'authenticated' ? current : 'unavailable'));
      throw error;
    }
  }, []);

  useEffect(() => {
    void refresh().catch(() => undefined);
  }, [refresh]);

  const login = useCallback(async (username: string, password: string) => {
    const nextSession = await loginRequest(username, password);
    setSession(nextSession);
    setError(null);
    setStatus('authenticated');
  }, []);

  const logout = useCallback(async () => {
    try {
      await logoutRequest();
    } finally {
      setSession(null);
      setError(null);
      setStatus('guest');
    }
  }, []);

  const value = useMemo(
    () => ({ status, session, error, refresh, login, logout }),
    [error, login, logout, refresh, session, status],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const value = useContext(AuthContext);

  if (!value) {
    throw new Error('Auth context is unavailable.');
  }

  return value;
}

export function ProtectedRoute({ children }: { children: ReactNode }) {
  const auth = useAuth();
  const location = useLocation();

  if (auth.status === 'loading') {
    return <CenteredLoader label="Checking session" />;
  }

  if (auth.status === 'unavailable') {
    return (
      <CenteredRetryState
        actionLabel="Retry"
        description={auth.error ?? 'The console could not verify your session right now.'}
        label="Unable to verify session"
        onAction={() => {
          void auth.refresh().catch(() => undefined);
        }}
      />
    );
  }

  if (auth.status !== 'authenticated') {
    return <Navigate replace state={{ from: `${location.pathname}${location.search}${location.hash}` }} to="/login" />;
  }

  return <>{children}</>;
}

export function GuestRoute({ children }: { children: ReactNode }) {
  const auth = useAuth();
  const location = useLocation();
  const redirectTarget = typeof location.state?.from === 'string' ? location.state.from : '/overview';

  if (auth.status === 'loading') {
    return <CenteredLoader label="Checking session" />;
  }

  if (auth.status === 'authenticated') {
    return <Navigate replace to={redirectTarget} />;
  }

  return <>{children}</>;
}

function CenteredLoader({ label }: { label: string }) {
  return (
    <div
      style={{
        minHeight: '100vh',
        display: 'grid',
        placeItems: 'center',
      }}
    >
      <Spin tip={label} />
    </div>
  );
}

function CenteredRetryState({
  label,
  description,
  actionLabel,
  onAction,
}: {
  label: string;
  description: string;
  actionLabel: string;
  onAction: () => void;
}) {
  const { token } = theme.useToken();

  return (
    <div
      style={{
        minHeight: '100vh',
        display: 'grid',
        placeItems: 'center',
        padding: 24,
      }}
    >
      <div
        style={{
          maxWidth: 420,
          display: 'grid',
          gap: 12,
          padding: 24,
          textAlign: 'center',
          borderRadius: token.borderRadiusLG,
          border: `1px solid ${token.colorBorderSecondary}`,
          background: token.colorBgContainer,
          boxShadow: token.boxShadowTertiary,
        }}
      >
        <strong style={{ color: token.colorText }}>{label}</strong>
        <span style={{ color: token.colorTextSecondary }}>{description}</span>
        <div>
          <Button onClick={onAction} type="primary">
            {actionLabel}
          </Button>
        </div>
      </div>
    </div>
  );
}
