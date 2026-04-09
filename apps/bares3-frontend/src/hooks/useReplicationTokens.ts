import { useCallback, useEffect, useState } from 'react';
import { listReplicationTokens, type SyncAccessToken } from '../api';
import { useAuth } from '../auth';

export function useReplicationTokens() {
  const auth = useAuth();
  const [tokens, setTokens] = useState<SyncAccessToken[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setTokens([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setTokens(await listReplicationTokens());
    } finally {
      setLoading(false);
    }
  }, [auth.status]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { tokens, loading, refresh, setTokens };
}
