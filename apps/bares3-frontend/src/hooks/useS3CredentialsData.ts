import { useCallback, useEffect, useState } from 'react';
import { listS3Credentials, type S3CredentialInfo } from '../api';
import { useAuth } from '../auth';

export function useS3CredentialsData() {
  const auth = useAuth();
  const [items, setItems] = useState<S3CredentialInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (auth.status !== 'authenticated') {
      setItems([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      setItems(await listS3Credentials());
    } finally {
      setLoading(false);
    }
  }, [auth.status]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { items, loading, refresh };
}
