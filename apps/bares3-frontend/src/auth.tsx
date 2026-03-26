import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { Spin } from 'antd';
import { ApiError, getSession, login as loginRequest, logout as logoutRequest } from './api';
import type { AuthSession } from './api';

type AuthStatus = 'loading' | 'authenticated' | 'guest';

type AuthContextValue = {
  status: AuthStatus;
  session: AuthSession | null;
  refresh: () => Promise<void>;
  login: (username: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>('loading');
  const [session, setSession] = useState<AuthSession | null>(null);

  const refresh = useCallback(async () => {
    setStatus((current) => (current === 'authenticated' ? current : 'loading'));
    try {
      const nextSession = await getSession();
      setSession(nextSession);
      setStatus('authenticated');
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        setSession(null);
        setStatus('guest');
        return;
      }

      setSession(null);
      setStatus('guest');
      throw error;
    }
  }, []);

  useEffect(() => {
    void refresh().catch(() => undefined);
  }, [refresh]);

  const login = useCallback(async (username: string, password: string) => {
    const nextSession = await loginRequest(username, password);
    setSession(nextSession);
    setStatus('authenticated');
  }, []);

  const logout = useCallback(async () => {
    try {
      await logoutRequest();
    } finally {
      setSession(null);
      setStatus('guest');
    }
  }, []);

  const value = useMemo(
    () => ({ status, session, refresh, login, logout }),
    [login, logout, refresh, session, status],
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

  if (auth.status !== 'authenticated') {
    return <Navigate replace state={{ from: location.pathname }} to="/login" />;
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
