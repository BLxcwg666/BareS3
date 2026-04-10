import { useCallback, useEffect, useMemo, useState } from 'react';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import { App as AntApp, Button, Checkbox, Empty, Input, Select, Skeleton, Space } from 'antd';
import { getDomainSettings, listBuckets, updateDomainSettings, type PublicDomainBinding } from '../api';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { normalizeApiError } from '../utils';

type DomainBindingDraft = PublicDomainBinding & {
  id: string;
};

let nextDomainBindingDraftID = 0;

function createDraft(binding?: Partial<PublicDomainBinding>): DomainBindingDraft {
  nextDomainBindingDraftID += 1;
  return {
    id: `domain-binding-${nextDomainBindingDraftID}`,
    host: binding?.host ?? '',
    bucket: binding?.bucket ?? '',
    prefix: binding?.prefix ?? '',
    index_document: binding?.index_document ?? true,
    spa_fallback: binding?.spa_fallback ?? false,
  };
}

export function DomainsPage() {
  const { message } = AntApp.useApp();
  const [bindings, setBindings] = useState<DomainBindingDraft[]>([]);
  const [savedBindings, setSavedBindings] = useState<PublicDomainBinding[]>([]);
  const [bucketNames, setBucketNames] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const [settings, buckets] = await Promise.all([getDomainSettings(), listBuckets()]);
      setBindings(settings.items.map((item) => createDraft(item)));
      setSavedBindings(settings.items);
      setBucketNames(buckets.map((bucket) => bucket.name));
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to load public domain bindings'));
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const normalizedBindings = useMemo(
    () => bindings.map(({ host, bucket, prefix, index_document, spa_fallback }) => ({
      host: host.trim(),
      bucket: bucket.trim(),
      prefix: prefix?.trim() || undefined,
      index_document: Boolean(index_document),
      spa_fallback: Boolean(spa_fallback),
    })),
    [bindings],
  );

  const dirty = JSON.stringify(normalizedBindings) !== JSON.stringify(savedBindings);

  const handleSave = async () => {
    setSaving(true);
    try {
      const result = await updateDomainSettings({ items: normalizedBindings });
      setBindings(result.items.map((item) => createDraft(item)));
      setSavedBindings(result.items);
      message.success('Public domain bindings saved');
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to save public domain bindings'));
    } finally {
      setSaving(false);
    }
  };

  return (
    <ConsoleShell
      actions={
        <Space size={8} wrap>
          <Button icon={<PlusOutlined />} onClick={() => setBindings((current) => [...current, createDraft()])}>
            Add binding
          </Button>
          <Button disabled={!dirty || loading} loading={saving} onClick={() => void handleSave()} type="primary">
            Save
          </Button>
        </Space>
      }
    >
      <div className="workspace-stack">
        <Section
          title="Public domains"
          note="BareS3 only matches Host headers and object paths here. SSL termination, DNS, and reverse proxy rules stay outside BareS3."
        >
          {loading ? (
            <Skeleton active paragraph={{ rows: 8 }} title={false} />
          ) : bindings.length === 0 ? (
            <Empty description="No public domains configured yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          ) : (
            <div className="access-rules-stack">
              {bindings.map((binding) => (
                <div className="access-rule-row" key={binding.id}>
                  <div className="access-rule-fields">
                    <div className="access-rule-prefix">
                      <span className="path-label">Host</span>
                      <Input
                        onChange={(event) => {
                          const value = event.target.value;
                          setBindings((current) => current.map((entry) => (entry.id === binding.id ? { ...entry, host: value } : entry)));
                        }}
                        placeholder="cdn.example.com"
                        value={binding.host}
                      />
                    </div>
                    <div className="access-rule-action">
                      <span className="path-label">Bucket</span>
                      <Select
                        onChange={(value) => {
                          setBindings((current) => current.map((entry) => (entry.id === binding.id ? { ...entry, bucket: value } : entry)));
                        }}
                        options={bucketNames.map((bucket) => ({ label: bucket, value: bucket }))}
                        placeholder="Select bucket"
                        value={binding.bucket || undefined}
                      />
                    </div>
                    <div className="access-rule-note">
                      <span className="path-label">Prefix</span>
                      <Input
                        onChange={(event) => {
                          const value = event.target.value;
                          setBindings((current) => current.map((entry) => (entry.id === binding.id ? { ...entry, prefix: value } : entry)));
                        }}
                        placeholder="site"
                        value={binding.prefix}
                      />
                    </div>
                    <div className="access-rule-note">
                      <span className="path-label">Fallbacks</span>
                      <Space direction="vertical" size={4}>
                        <Checkbox
                          checked={Boolean(binding.index_document)}
                          onChange={(event) => {
                            const checked = event.target.checked;
                            setBindings((current) => current.map((entry) => (
                              entry.id === binding.id
                                ? { ...entry, index_document: checked, spa_fallback: checked ? entry.spa_fallback : false }
                                : entry
                            )));
                          }}
                        >
                          Route `/` to `index.html`
                        </Checkbox>
                        <Checkbox
                          checked={Boolean(binding.spa_fallback)}
                          disabled={!binding.index_document}
                          onChange={(event) => {
                            const checked = event.target.checked;
                            setBindings((current) => current.map((entry) => (entry.id === binding.id ? { ...entry, spa_fallback: checked } : entry)));
                          }}
                        >
                          SPA fallback to `index.html`
                        </Checkbox>
                      </Space>
                    </div>
                  </div>

                  <div className="access-rule-actions">
                    <Button danger icon={<DeleteOutlined />} onClick={() => setBindings((current) => current.filter((entry) => entry.id !== binding.id))} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </Section>

        <Section title="How it works" note="Requests are served from the file endpoint after your reverse proxy forwards the Host header.">
          <div className="row-note"><code>https://cdn.example.com/logo.svg</code> maps to the selected bucket plus the optional prefix plus <code>logo.svg</code>.</div>
          <div className="row-note">You can toggle whether `/` resolves to <code>index.html</code> and whether missing routes fall back to that same file.</div>
          <div className="row-note">Anonymous reads still obey the bucket's public access rules.</div>
        </Section>
      </div>
    </ConsoleShell>
  );
}
