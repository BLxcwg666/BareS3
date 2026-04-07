import { useCallback, useEffect, useState } from 'react';
import { getObject, type ObjectInfo } from '../api';
import { useAuth } from '../auth';

export function useObjectDetail(bucket: string | null, key: string | null) {
  const auth = useAuth();
  const [item, setItem] = useState<ObjectInfo | null>(null);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated' || !bucket || !key) {
      setItem(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setItem(await getObject(bucket, key));
    } finally {
      setLoading(false);
    }
  }, [auth.status, bucket, key]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { item, loading, refresh };
}
