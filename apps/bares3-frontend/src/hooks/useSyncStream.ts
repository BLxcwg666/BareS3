import { useCallback, useEffect, useState } from 'react';
import { getSyncSettings, listReplicationRemotes, type ReplicationRemote, type SyncSettings } from '../api';
import { useAuth } from '../auth';

type SyncStreamSnapshot = {
  type: 'snapshot';
  settings: SyncSettings;
  remotes: ReplicationRemote[];
  at: string;
};

const reconnectDelayMs = 2000;

function syncStreamURL() {
  const url = new URL('/api/v1/replication/stream', window.location.origin);
  url.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.toString();
}

export function useSyncStream() {
  const auth = useAuth();
  const [settings, setSettings] = useState<SyncSettings | null>(null);
  const [remotes, setRemotes] = useState<ReplicationRemote[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setSettings(null);
      setRemotes([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const [nextSettings, nextRemotes] = await Promise.all([getSyncSettings(), listReplicationRemotes()]);
      setSettings(nextSettings);
      setRemotes(nextRemotes);
    } finally {
      setLoading(false);
    }
  }, [auth.status]);

  useEffect(() => {
    if (auth.status !== 'authenticated') {
      setSettings(null);
      setRemotes([]);
      setLoading(false);
      return undefined;
    }

    let cancelled = false;
    let socket: WebSocket | null = null;
    let reconnectTimer: number | null = null;

    const connect = () => {
      if (cancelled) {
        return;
      }
      socket = new WebSocket(syncStreamURL());
      socket.onmessage = (event) => {
        const snapshot = JSON.parse(event.data) as SyncStreamSnapshot;
        if (snapshot.type !== 'snapshot') {
          return;
        }
        setSettings(snapshot.settings);
        setRemotes(snapshot.remotes);
        setLoading(false);
      };
      socket.onclose = () => {
        if (cancelled) {
          return;
        }
        reconnectTimer = window.setTimeout(connect, reconnectDelayMs);
      };
      socket.onerror = () => {
        socket?.close();
      };
    };

    void refresh();
    connect();

    return () => {
      cancelled = true;
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
      }
      socket?.close();
    };
  }, [auth.status, refresh]);

  return { settings, remotes, loading, refresh, setSettings, setRemotes };
}
