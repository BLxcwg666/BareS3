import { useEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { SearchOutlined } from '@ant-design/icons';
import { AutoComplete, Input, Spin } from 'antd';
import { useNavigate } from 'react-router-dom';
import { listBuckets, listObjects } from '../api';
import { useAuth } from '../auth';

type SearchHit = {
  kind: 'bucket' | 'object';
  bucket: string;
  key?: string;
};

type SearchOption = {
  value: string;
  label: ReactNode;
};

function buildOptionLabel(title: string, note: string) {
  return (
    <div>
      <div className="row-title row-title-small">{title}</div>
      <div className="row-note">{note}</div>
    </div>
  );
}

export function GlobalSearch() {
  const auth = useAuth();
  const navigate = useNavigate();
  const [query, setQuery] = useState('');
  const [options, setOptions] = useState<SearchOption[]>([]);
  const [searching, setSearching] = useState(false);
  const requestIdRef = useRef(0);

  useEffect(() => {
    if (auth.status !== 'authenticated') {
      setOptions([]);
      setSearching(false);
      return;
    }

    const keyword = query.trim().toLowerCase();
    if (keyword.length < 2) {
      setOptions([]);
      setSearching(false);
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    const timer = window.setTimeout(() => {
      void (async () => {
        setSearching(true);
        try {
          const buckets = await listBuckets();
          const nextOptions: SearchOption[] = [];

          for (const bucket of buckets) {
            if (bucket.name.toLowerCase().includes(keyword)) {
              const payload = JSON.stringify({ kind: 'bucket', bucket: bucket.name } satisfies SearchHit);
              nextOptions.push({
                value: payload,
                label: buildOptionLabel(bucket.name, 'Bucket'),
              });
            }
          }

          if (nextOptions.length < 12) {
            for (const bucket of buckets) {
              const objects = await listObjects(bucket.name, undefined, 200);
              for (const object of objects) {
                if (!object.key.toLowerCase().includes(keyword)) {
                  continue;
                }

                const payload = JSON.stringify({ kind: 'object', bucket: bucket.name, key: object.key } satisfies SearchHit);
                nextOptions.push({
                  value: payload,
                  label: buildOptionLabel(object.key, bucket.name),
                });

                if (nextOptions.length >= 12) {
                  break;
                }
              }

              if (nextOptions.length >= 12) {
                break;
              }
            }
          }

          if (requestIdRef.current === requestId) {
            setOptions(nextOptions);
          }
        } finally {
          if (requestIdRef.current === requestId) {
            setSearching(false);
          }
        }
      })();
    }, 220);

    return () => {
      window.clearTimeout(timer);
    };
  }, [auth.status, query]);

  const notFoundContent = useMemo(
    () => (searching ? <Spin size="small" /> : query.trim().length >= 2 ? 'No matches' : null),
    [query, searching],
  );

  return (
    <AutoComplete
      className="header-search"
      notFoundContent={notFoundContent}
      onChange={(value) => setQuery(value)}
      onSelect={(value) => {
        const hit = JSON.parse(value) as SearchHit;
        if (hit.kind === 'bucket') {
          navigate(`/browser?bucket=${encodeURIComponent(hit.bucket)}`);
        } else {
          navigate(
            `/browser?bucket=${encodeURIComponent(hit.bucket)}&key=${encodeURIComponent(hit.key ?? '')}&q=${encodeURIComponent(hit.key ?? '')}`,
          );
        }
        setQuery('');
        setOptions([]);
      }}
      options={options}
      value={query}
    >
      <Input allowClear placeholder="Search bucket or key" prefix={<SearchOutlined />} />
    </AutoComplete>
  );
}
