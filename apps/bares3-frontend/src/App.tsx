import { lazy, Suspense } from 'react';
import { Spin } from 'antd';
import { HashRouter, Navigate, Route, Routes } from 'react-router-dom';
import { AuthProvider, GuestRoute, ProtectedRoute } from './auth';
import { ThemeProvider } from './theme';

const LoginPage = lazy(async () => {
  const module = await import('./pages/LoginPage');
  return { default: module.LoginPage };
});

const OverviewPage = lazy(async () => {
  const module = await import('./pages/OverviewPage');
  return { default: module.OverviewPage };
});

const BucketsPage = lazy(async () => {
  const module = await import('./pages/BucketsPage');
  return { default: module.BucketsPage };
});

const BucketAccessPage = lazy(async () => {
  const module = await import('./pages/BucketAccessPage');
  return { default: module.BucketAccessPage };
});

const AccessKeysPage = lazy(async () => {
  const module = await import('./pages/AccessKeysPage');
  return { default: module.AccessKeysPage };
});

const BrowserPage = lazy(async () => {
  const module = await import('./pages/BrowserPage');
  return { default: module.BrowserPage };
});

const AuditLogsPage = lazy(async () => {
  const module = await import('./pages/AuditLogsPage');
  return { default: module.AuditLogsPage };
});

const LinksPage = lazy(async () => {
  const module = await import('./pages/LinksPage');
  return { default: module.LinksPage };
});

const SettingsPage = lazy(async () => {
  const module = await import('./pages/SettingsPage');
  return { default: module.SettingsPage };
});

function RouteFallback() {
  return (
    <div
      style={{
        minHeight: '100vh',
        display: 'grid',
        placeItems: 'center',
      }}
    >
      <Spin tip="Loading page" />
    </div>
  );
}

export default function App() {
  return (
    <ThemeProvider>
      <HashRouter>
        <AuthProvider>
          <Suspense fallback={<RouteFallback />}>
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
                path="/buckets/:bucket/access"
                element={
                  <ProtectedRoute>
                    <BucketAccessPage />
                  </ProtectedRoute>
                }
              />
              <Route
                path="/access-keys"
                element={
                  <ProtectedRoute>
                    <AccessKeysPage />
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
          </Suspense>
        </AuthProvider>
      </HashRouter>
    </ThemeProvider>
  );
}
