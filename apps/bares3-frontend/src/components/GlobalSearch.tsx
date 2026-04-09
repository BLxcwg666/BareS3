import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { CloudServerOutlined, EnterOutlined, FileOutlined, FolderOpenOutlined, SearchOutlined } from '@ant-design/icons';
import { Button, Input, Modal, Spin, Tag, Typography } from 'antd';
import type { InputRef } from 'antd';
import { useNavigate } from 'react-router-dom';
import { searchStorage, type SearchHit } from '../api';
import { useAuth } from '../auth';

const { Text } = Typography;

export function GlobalSearch() {
  const auth = useAuth();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [searching, setSearching] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const requestIdRef = useRef(0);
  const inputRef = useRef<InputRef>(null);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        setOpen((prev) => !prev);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, []);

  useEffect(() => {
    if (open) {
      setQuery('');
      setHits([]);
      setActiveIndex(0);
      setSearching(false);
      // Ensure input gets focused when modal opens
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [open]);

  useEffect(() => {
    setActiveIndex(0);

    if (auth.status !== 'authenticated') {
      setHits([]);
      setSearching(false);
      return;
    }

    const keyword = query.trim().toLowerCase();
    if (keyword.length < 2) {
      setHits([]);
      setSearching(false);
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    const timer = window.setTimeout(() => {
      void (async () => {
        setSearching(true);
        try {
          const results = await searchStorage(keyword, 12);
          if (requestIdRef.current === requestId) {
            setHits(results);
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

  const handleSelect = useCallback(
    (hit: SearchHit) => {
      if (hit.kind === 'bucket') {
        navigate(`/browser?bucket=${encodeURIComponent(hit.bucket)}`);
      } else {
        const key = hit.key ?? '';
        const isFolder = key.endsWith('/');
        let path = '';
        if (isFolder) {
          path = key;
          navigate(`/browser?bucket=${encodeURIComponent(hit.bucket)}&path=${encodeURIComponent(path)}`);
        } else {
          const lastSlashIndex = key.lastIndexOf('/');
          path = lastSlashIndex !== -1 ? key.substring(0, lastSlashIndex + 1) : '';
          navigate(
            `/browser?bucket=${encodeURIComponent(hit.bucket)}&path=${encodeURIComponent(path)}&key=${encodeURIComponent(key)}`,
          );
        }
      }
      setOpen(false);
    },
    [navigate],
  );

  const handleInputKeyDown = (e: React.KeyboardEvent) => {
    if (hits.length === 0) return;

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActiveIndex((prev) => (prev + 1) % hits.length);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActiveIndex((prev) => (prev - 1 + hits.length) % hits.length);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const hit = hits[activeIndex];
      if (hit) {
        handleSelect(hit);
      }
    }
  };

  return (
    <>
      <Button
        className="header-search"
        onClick={() => setOpen(true)}
        style={{ display: 'flex', alignItems: 'center', color: 'var(--muted)' }}
      >
        <SearchOutlined />
        <span style={{ flex: 1, textAlign: 'left' }}>Search...</span>
        <Tag style={{ marginInlineEnd: 0, border: 'none', background: 'var(--surface-soft)' }}>Ctrl K</Tag>
      </Button>

      <Modal
        className="cmdk-modal"
        closable={false}
        footer={null}
        open={open}
        onCancel={() => setOpen(false)}
        width={600}
        style={{ top: '15vh' }}
        maskStyle={{ backdropFilter: 'blur(4px)' }}
        destroyOnClose
      >
        <div className="cmdk-header">
          <SearchOutlined style={{ fontSize: 18, color: 'var(--muted)' }} />
          <Input
            ref={inputRef}
            className="cmdk-input"
            placeholder="Search bucket or key"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleInputKeyDown}
            allowClear
          />
          {searching && <Spin size="small" />}
        </div>
        
        {(query.trim().length >= 2 || hits.length > 0) && (
          <ul className="cmdk-list">
            {hits.length === 0 && !searching && (
              <div style={{ padding: '24px', textAlign: 'center', color: 'var(--muted)' }}>
                No matches found
              </div>
            )}
            
            {hits.map((hit, i) => {
              const isBucket = hit.kind === 'bucket';
              const isFolder = hit.key?.endsWith('/');
              const title = isBucket ? hit.bucket : hit.key ?? hit.bucket;
              const note = isBucket ? 'Bucket' : hit.bucket;
              
              let icon = <FileOutlined />;
              if (isBucket) icon = <CloudServerOutlined />;
              else if (isFolder) icon = <FolderOpenOutlined />;

              return (
                <li
                  key={`${hit.kind}-${hit.bucket}-${hit.key ?? ''}`}
                  className={`cmdk-item ${i === activeIndex ? 'cmdk-item-active' : ''}`}
                  onClick={() => handleSelect(hit)}
                  onMouseEnter={() => setActiveIndex(i)}
                >
                  <div className="cmdk-item-icon">{icon}</div>
                  <div className="cmdk-item-content">
                    <div className="cmdk-item-title">{title}</div>
                    <div className="cmdk-item-note">{note}</div>
                  </div>
                  <div className="cmdk-item-action">
                    <EnterOutlined />
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </Modal>
    </>
  );
}
