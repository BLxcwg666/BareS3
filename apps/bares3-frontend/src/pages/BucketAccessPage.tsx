import { useCallback, useEffect, useMemo, useState } from 'react';
import { ArrowDownOutlined, ArrowLeftOutlined, ArrowUpOutlined, DeleteOutlined, FolderOpenOutlined, PlusOutlined } from '@ant-design/icons';
import { App as AntApp, Button, Descriptions, Empty, Input, Select, Skeleton, Space } from 'antd';
import { useNavigate, useParams } from 'react-router-dom';
import { getBucketAccessConfig, listBuckets, updateBucketAccessConfig, type BucketAccessAction, type BucketAccessRule, type BucketInfo } from '../api';
import { ExposureTag } from '../components/ExposureTag';
import { ConsoleShell } from '../components/ConsoleShell';
import { Section } from '../components/Section';
import { bucketAccessActionOptions, bucketAccessModeOptions } from '../constants';
import { bucketAccessActionLabel, bucketAccessModeLabel, normalizeApiError } from '../utils';

type RuleDraft = BucketAccessRule & {
  id: string;
};

let nextRuleDraftID = 0;

function createRuleDraft(rule?: Partial<BucketAccessRule>): RuleDraft {
  nextRuleDraftID += 1;
  return {
    id: `rule-${nextRuleDraftID}`,
    prefix: rule?.prefix ?? '',
    action: rule?.action ?? 'authenticated',
    note: rule?.note ?? '',
  };
}

function modeDescription(mode: string) {
  switch (mode) {
    case 'public':
      return 'Every object can be read without authentication.';
    case 'custom':
      return 'Ordered prefix rules decide which paths are public, authenticated, or denied.';
    default:
      return 'Every object requires authentication unless you grant a more specific rule later.';
  }
}

export function BucketAccessPage() {
  const { message } = AntApp.useApp();
  const navigate = useNavigate();
  const params = useParams();
  const bucketName = params.bucket ?? '';
  const [bucket, setBucket] = useState<BucketInfo | null>(null);
  const [mode, setMode] = useState<'private' | 'public' | 'custom'>('private');
  const [defaultAction, setDefaultAction] = useState<BucketAccessAction>('authenticated');
  const [rules, setRules] = useState<RuleDraft[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [loaded, setLoaded] = useState(false);

  const refresh = useCallback(async () => {
    if (!bucketName) {
      setBucket(null);
      setLoading(false);
      setLoaded(true);
      return;
    }

    setLoading(true);
    try {
      const [items, access] = await Promise.all([listBuckets(), getBucketAccessConfig(bucketName)]);
      const matchedBucket = items.find((item) => item.name === bucketName) ?? null;
      setBucket(matchedBucket);
      setMode(access.mode);
      setDefaultAction(access.policy.default_action);
      setRules(access.policy.rules.map((rule) => createRuleDraft(rule)));
    } catch (error) {
      setBucket(null);
      message.error(normalizeApiError(error, 'Failed to load bucket access rules'));
    } finally {
      setLoading(false);
      setLoaded(true);
    }
  }, [bucketName, message]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const handleSave = async () => {
    if (!bucketName) {
      return;
    }

    setSaving(true);
    try {
      const updated = await updateBucketAccessConfig(bucketName, {
        mode,
        policy: {
          default_action: defaultAction,
          rules: rules.map(({ prefix, action, note }) => ({
            prefix: prefix.trim(),
            action,
            note: note?.trim() || undefined,
          })),
        },
      });
      setMode(updated.mode);
      setDefaultAction(updated.policy.default_action);
      setRules(updated.policy.rules.map((rule) => createRuleDraft(rule)));
      message.success(`Access rules for ${bucketName} saved`);
      await refresh();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to save bucket access rules'));
    } finally {
      setSaving(false);
    }
  };

  const semanticsItems = useMemo(
    () => [
      { key: 'public', label: 'Public', children: 'Matches can be read through /pub without authentication.' },
      { key: 'authenticated', label: 'Require auth', children: 'Matches are available to signed S3 requests and share links, but not anonymous /pub reads.' },
      { key: 'deny', label: 'Deny', children: 'Matches are blocked even when a signed request or share link is used.' },
    ],
    [],
  );

  const hasRules = rules.length > 0;

  return (
    <ConsoleShell
      actions={
        <Space size={8} wrap>
          <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/buckets')}>
            Buckets
          </Button>
          <Button icon={<FolderOpenOutlined />} onClick={() => navigate({ pathname: '/browser', search: `?bucket=${encodeURIComponent(bucketName)}` })}>
            Browse
          </Button>
          <Button loading={saving} onClick={() => void handleSave()} type="primary">
            Save access
          </Button>
        </Space>
      }
      meta={{ title: 'Bucket Access', note: bucketName ? `Rules for ${bucketName}` : 'Rules for bucket reads and public delivery.' }}
    >
      <div className="workspace-stack">
        {loading ? (
          <Section title="Access policy">
            <Skeleton active paragraph={{ rows: 10 }} title={false} />
          </Section>
        ) : !bucketName || (!bucket && loaded) ? (
          <Section title="Access policy">
            <Empty description="Bucket access rules are unavailable" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          </Section>
        ) : (
          <>
            <div className="workspace-grid workspace-grid-main access-page-grid">
              <Section title="Effective mode" note="Private and public are shortcuts. Custom enables ordered rules.">
                <div className="access-mode-stack">
                  <div className="bucket-summary-grid">
                    <div className="bucket-stat">
                      <span className="path-label">Bucket</span>
                      <strong className="bucket-stat-value">{bucket?.name}</strong>
                    </div>
                    <div className="bucket-stat">
                      <span className="path-label">Mode</span>
                      <div>
                        <ExposureTag value={bucketAccessModeLabel(mode)} />
                      </div>
                    </div>
                    <div className="bucket-stat">
                      <span className="path-label">Root</span>
                      <strong className="bucket-stat-value">{bucket?.path}</strong>
                    </div>
                  </div>

                  <Select options={bucketAccessModeOptions} onChange={setMode} value={mode} />
                  <div className="row-note">{modeDescription(mode)}</div>
                  {mode !== 'custom' && hasRules ? <div className="row-note">Saved custom rules stay parked until you switch this bucket back to Custom.</div> : null}
                </div>
              </Section>

              <Section title="Rule semantics" note="First matching rule wins. Unmatched paths use the default action.">
                <Descriptions column={1} items={semanticsItems} size="small" />
              </Section>
            </div>

            <Section
              title="Custom rules"
              note="Use prefixes like images/, docs/public/, or reports/q4.pdf. Prefixes are matched from top to bottom."
              extra={
                <Button disabled={mode !== 'custom'} icon={<PlusOutlined />} onClick={() => setRules((current) => [...current, createRuleDraft()])}>
                  Add rule
                </Button>
              }
            >
              {mode !== 'custom' ? (
                <Empty description="Switch this bucket to Custom to edit ordered access rules" image={Empty.PRESENTED_IMAGE_SIMPLE} />
              ) : (
                <div className="access-rules-stack">
                  <div className="access-default-row">
                    <span className="path-label">Default action</span>
                    <Select options={bucketAccessActionOptions} onChange={setDefaultAction} value={defaultAction} />
                    <span className="row-note">Applies when no custom rule matches.</span>
                  </div>

                  {!hasRules ? (
                    <Empty description="No custom rules yet. Unmatched paths will use the default action." image={Empty.PRESENTED_IMAGE_SIMPLE} />
                  ) : (
                    rules.map((rule, index) => (
                      <div className="access-rule-row" key={rule.id}>
                        <div className="access-rule-fields">
                          <div className="access-rule-prefix">
                            <span className="path-label">Prefix</span>
                            <Input
                              onChange={(event) => {
                                const value = event.target.value;
                                setRules((current) => current.map((entry) => (entry.id === rule.id ? { ...entry, prefix: value } : entry)));
                              }}
                              placeholder="images/"
                              value={rule.prefix}
                            />
                          </div>
                          <div className="access-rule-action">
                            <span className="path-label">Action</span>
                            <Select
                              onChange={(value) => {
                                setRules((current) => current.map((entry) => (entry.id === rule.id ? { ...entry, action: value } : entry)));
                              }}
                              options={bucketAccessActionOptions}
                              value={rule.action}
                            />
                          </div>
                          <div className="access-rule-note">
                            <span className="path-label">Note</span>
                            <Input
                              onChange={(event) => {
                                const value = event.target.value;
                                setRules((current) => current.map((entry) => (entry.id === rule.id ? { ...entry, note: value } : entry)));
                              }}
                              placeholder={`${bucketAccessActionLabel(rule.action)} access for this prefix`}
                              value={rule.note}
                            />
                          </div>
                        </div>

                        <div className="access-rule-actions">
                          <Button
                            disabled={index === 0}
                            icon={<ArrowUpOutlined />}
                            onClick={() => {
                              if (index === 0) {
                                return;
                              }
                              setRules((current) => {
                                const next = current.slice();
                                [next[index - 1], next[index]] = [next[index], next[index - 1]];
                                return next;
                              });
                            }}
                          />
                          <Button
                            disabled={index === rules.length - 1}
                            icon={<ArrowDownOutlined />}
                            onClick={() => {
                              if (index === rules.length - 1) {
                                return;
                              }
                              setRules((current) => {
                                const next = current.slice();
                                [next[index], next[index + 1]] = [next[index + 1], next[index]];
                                return next;
                              });
                            }}
                          />
                          <Button danger icon={<DeleteOutlined />} onClick={() => setRules((current) => current.filter((entry) => entry.id !== rule.id))} />
                        </div>
                      </div>
                    ))
                  )}
                </div>
              )}
            </Section>
          </>
        )}
      </div>
    </ConsoleShell>
  );
}
