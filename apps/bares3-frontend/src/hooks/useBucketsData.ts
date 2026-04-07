import { useCallback, useEffect, useState } from 'react';
import { listBuckets, type BucketInfo } from '../api';
import { useAuth } from '../auth';

export function useBucketsData() {
  const auth = useAuth();
  const [items, setItems] = useState<BucketInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setItems([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setItems(await listBuckets());
    } finally {
      setLoading(false);
    }
  }, [auth.status]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, loading, refresh };
}
