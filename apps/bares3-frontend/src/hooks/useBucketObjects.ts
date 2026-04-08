import { useCallback, useEffect, useRef, useState } from 'react';
import { listObjects, type ObjectInfo } from '../api';
import { useAuth } from '../auth';

const pageSize = 200;

export function useBucketObjects(bucket: string | null, prefix?: string | null, query?: string | null) {
  const auth = useAuth();
  const [items, setItems] = useState<ObjectInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [hasMore, setHasMore] = useState(false);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const requestIdRef = useRef(0);

  const loadPage = useCallback(async (cursor?: string, append = false) => {
    if (auth.status !== 'authenticated' || !bucket) {
      setItems([]);
      setLoading(false);
      setLoadingMore(false);
      setHasMore(false);
      setNextCursor(null);
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    if (append) {
      setLoadingMore(true);
    } else {
      setLoading(true);
    }

    try {
      const payload = await listObjects(bucket, {
        prefix: prefix ?? undefined,
        query: query ?? undefined,
        cursor,
        limit: pageSize,
      });

      if (requestIdRef.current !== requestId) {
        return;
      }

      setItems((current) => (append ? [...current, ...payload.items] : payload.items));
      setHasMore(payload.has_more);
      setNextCursor(payload.next_cursor ?? null);
    } finally {
      if (requestIdRef.current === requestId) {
        if (append) {
          setLoadingMore(false);
        } else {
          setLoading(false);
        }
      }
    }
  }, [auth.status, bucket, prefix, query]);

  const refresh = useCallback(async () => {
    await loadPage(undefined, false);
  }, [loadPage]);

  const loadMore = useCallback(async () => {
    if (!nextCursor || !hasMore || loading || loadingMore) {
      return;
    }
    await loadPage(nextCursor, true);
  }, [hasMore, loadPage, loading, loadingMore, nextCursor]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, loading, loadingMore, hasMore, refresh, loadMore };
}
