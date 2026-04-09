import { useCallback, useEffect, useState } from 'react';
import { listReplicationRemotes, listReplicationTokens, type ReplicationRemote, type SyncAccessToken } from '../api';
import { useAuth } from '../auth';

const refreshIntervalMs = 3000;

export function useReplicationData() {
  const auth = useAuth();
  const [tokens, setTokens] = useState<SyncAccessToken[]>([]);
  const [remotes, setRemotes] = useState<ReplicationRemote[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async (quiet = false) => {
    if (auth.status !== 'authenticated') {
      setTokens([]);
      setRemotes([]);
      setLoading(false);
      return;
    }

    if (!quiet) {
      setLoading(true);
    }
    try {
      const [nextTokens, nextRemotes] = await Promise.all([listReplicationTokens(), listReplicationRemotes()]);
      setTokens(nextTokens);
      setRemotes(nextRemotes);
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

  return { tokens, remotes, loading, refresh, setTokens, setRemotes };
}
