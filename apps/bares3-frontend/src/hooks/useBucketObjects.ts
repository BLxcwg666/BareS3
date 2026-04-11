import { useCallback, useEffect, useRef, useState } from 'react';
import { listObjects, type ObjectInfo } from '../api';
import { useAuth } from '../auth';

export const bucketObjectsPageSize = 15;

export function useBucketObjects(bucket: string | null, prefix?: string | null, query?: string | null, page = 1, pageSize = bucketObjectsPageSize) {
  const auth = useAuth();
  const [items, setItems] = useState<ObjectInfo[]>([]);
  const [prefixes, setPrefixes] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const requestIdRef = useRef(0);

  const loadPage = useCallback(async () => {
    if (auth.status !== 'authenticated' || !bucket) {
      setItems([]);
      setPrefixes([]);
      setTotalCount(0);
      setLoading(false);
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    setLoading(true);

    try {
      const payload = await listObjects(bucket, {
        prefix: prefix ?? undefined,
        query: query ?? undefined,
        delimiter: '/',
        offset: Math.max(0, page - 1) * pageSize,
        limit: pageSize,
      });

      if (requestIdRef.current !== requestId) {
        return;
      }

      setItems(payload.items);
      setPrefixes(payload.prefixes ?? []);
      setTotalCount(payload.total_count ?? payload.items.length + (payload.prefixes?.length ?? 0));
    } finally {
      if (requestIdRef.current === requestId) {
        setLoading(false);
      }
    }
  }, [auth.status, bucket, page, pageSize, prefix, query]);

  const refresh = useCallback(async () => {
    await loadPage();
  }, [loadPage]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, prefixes, totalCount, loading, refresh };
}
