import { useCallback, useEffect, useState } from 'react';
import { listAuditEntries, type AuditEntry } from '../api';
import { useAuth } from '../auth';

export function useAuditActivity(limit = 8) {
  const auth = useAuth();
  const [items, setItems] = useState<AuditEntry[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setItems([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setItems(await listAuditEntries(limit));
    } finally {
      setLoading(false);
    }
  }, [auth.status, limit]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, loading, refresh };
}
