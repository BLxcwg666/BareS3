import { useCallback, useEffect, useState } from 'react';
import { listObjects, type ObjectInfo } from '../api';
import { useAuth } from '../auth';

export function useBucketObjects(bucket: string | null, prefix?: string | null) {
  const auth = useAuth();
  const [items, setItems] = useState<ObjectInfo[]>([]);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated' || !bucket) {
      setItems([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setItems(await listObjects(bucket, prefix ?? undefined));
    } finally {
      setLoading(false);
    }
  }, [auth.status, bucket, prefix]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, loading, refresh };
}
