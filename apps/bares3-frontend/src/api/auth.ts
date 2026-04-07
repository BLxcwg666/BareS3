import { request } from './client';

export type AuthSession = {
  username: string;
  expires_at: string;
};

export function getSession() {
  return request<AuthSession>('/api/v1/auth/me');
}

export function login(username: string, password: string) {
  return request<AuthSession>('/api/v1/auth/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ username, password }),
  });
}

export function logout() {
  return request<void>('/api/v1/auth/logout', {
    method: 'POST',
  });
}
