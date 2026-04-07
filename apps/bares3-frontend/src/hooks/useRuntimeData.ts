import { useCallback, useEffect, useState } from 'react';
import { getRuntime, type RuntimeInfo } from '../api';
import { useAuth } from '../auth';

export function useRuntimeData() {
  const auth = useAuth();
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setRuntime(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setRuntime(await getRuntime());
    } finally {
      setLoading(false);
    }
  }, [auth.status]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { runtime, loading, refresh };
}
