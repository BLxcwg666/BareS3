import { useCallback, useEffect, useState } from 'react';
import { getSyncSettings, type SyncSettings } from '../api';
import { useAuth } from '../auth';

const refreshIntervalMs = 3000;

export function useSyncSettings() {
  const auth = useAuth();
  const [settings, setSettings] = useState<SyncSettings | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async (quiet = false) => {
    if (auth.status !== 'authenticated') {
      setSettings(null);
      setLoading(false);
      return;
    }

    if (!quiet) {
      setLoading(true);
    }
    try {
      setSettings(await getSyncSettings());
    } finally {
		if (!quiet) {
			setLoading(false);
		}
    }
  }, [auth.status]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (auth.status !== 'authenticated') {
      return undefined;
    }

    const handle = window.setInterval(() => {
      void refresh(true);
    }, refreshIntervalMs);
    return () => window.clearInterval(handle);
  }, [auth.status, refresh]);

  return { settings, loading, refresh, setSettings };
}
