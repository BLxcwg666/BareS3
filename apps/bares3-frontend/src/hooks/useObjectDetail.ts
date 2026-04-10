import { useCallback, useEffect, useRef, useState } from 'react';
import { getObject, type ObjectInfo } from '../api';
import { useAuth } from '../auth';

export function useObjectDetail(bucket: string | null, key: string | null) {
  const auth = useAuth();
  const [item, setItem] = useState<ObjectInfo | null>(null);
  const [loading, setLoading] = useState(false);
  const requestIdRef = useRef(0);
  const latestRef = useRef({ authStatus: auth.status, bucket, key });

  latestRef.current = { authStatus: auth.status, bucket, key };

  const refresh = useCallback(async () => {
    const { authStatus, bucket: nextBucket, key: nextKey } = latestRef.current;
    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    if (authStatus !== 'authenticated' || !nextBucket || !nextKey) {
      setItem(null);
      setLoading(false);
      return;
    }

    setItem((current) =>
      current && current.bucket === nextBucket && current.key === nextKey ? current : null,
    );
    setLoading(true);
    try {
      const nextItem = await getObject(nextBucket, nextKey);
      if (requestIdRef.current !== requestId) {
        return;
      }

      const latest = latestRef.current;
      if (latest.authStatus !== 'authenticated' || latest.bucket !== nextBucket || latest.key !== nextKey) {
        return;
      }

      setItem(nextItem);
    } finally {
      if (requestIdRef.current === requestId) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [auth.status, bucket, key, refresh]);

  return { item, loading, refresh };
}
