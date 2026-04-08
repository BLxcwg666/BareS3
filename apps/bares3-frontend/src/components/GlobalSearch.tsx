import { useEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { SearchOutlined } from '@ant-design/icons';
import { AutoComplete, Input, Spin } from 'antd';
import { useNavigate } from 'react-router-dom';
import { searchStorage, type SearchHit } from '../api';
import { useAuth } from '../auth';

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
          const hits = await searchStorage(keyword, 12);
          const nextOptions: SearchOption[] = hits.map((hit) => ({
            value: JSON.stringify(hit satisfies SearchHit),
            label: buildOptionLabel(hit.kind === 'bucket' ? hit.bucket : hit.key ?? hit.bucket, hit.kind === 'bucket' ? 'Bucket' : hit.bucket),
          }));

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
