import { HashRouter, Navigate, Route, Routes } from 'react-router-dom';
import { AuthProvider, GuestRoute, ProtectedRoute } from './auth';
import { ThemeProvider } from './theme';
import { AuditLogsPage } from './pages/AuditLogsPage';
import { BrowserPage } from './pages/BrowserPage';
import { BucketsPage } from './pages/BucketsPage';
import { LinksPage } from './pages/LinksPage';
import { LoginPage } from './pages/LoginPage';
import { OverviewPage } from './pages/OverviewPage';
import { SettingsPage } from './pages/SettingsPage';

export default function App() {
  return (
    <ThemeProvider>
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
    </ThemeProvider>
  );
}
