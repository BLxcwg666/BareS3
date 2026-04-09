import { useEffect, useState } from 'react';
import { App as AntApp, Button, Descriptions, Form, Input, Modal, Select, Space } from 'antd';
import { createS3Credential, type CreatedS3Credential, type S3CredentialInfo, type S3CredentialPermission, updateS3Credential } from '../api';
import { s3CredentialPermissionOptions } from '../constants';
import { copyText, formatDateTime, normalizeApiError, s3CredentialBucketScopeLabel, s3CredentialPermissionLabel } from '../utils';

export function S3CredentialModal({
  open,
  onCancel,
  onSaved,
  bucketNames,
  credential,
}: {
  open: boolean;
  onCancel: () => void;
  onSaved: () => Promise<void> | void;
  bucketNames: string[];
  credential?: S3CredentialInfo | null;
}) {
  const { message } = AntApp.useApp();
  const [form] = Form.useForm<{ label: string; permission: S3CredentialPermission; buckets: string[] }>();
  const [submitting, setSubmitting] = useState(false);
  const [created, setCreated] = useState<CreatedS3Credential | null>(null);
  const editing = Boolean(credential);

  useEffect(() => {
    if (!open) {
      return;
    }

    setCreated(null);
    form.setFieldsValue({
      label: credential?.label ?? '',
      permission: credential?.permission ?? 'read_write',
      buckets: credential?.buckets ?? [],
    });
  }, [credential, form, open]);

  const handleSubmit = async () => {
    const values = await form.validateFields();
    setSubmitting(true);
    try {
      if (credential) {
        const updated = await updateS3Credential(credential.access_key_id, values.label.trim(), values.permission, values.buckets ?? []);
        message.success(`Updated ${updated.access_key_id}`);
        await onSaved();
        onCancel();
      } else {
        const next = await createS3Credential(values.label.trim(), values.permission, values.buckets ?? []);
        setCreated(next);
        message.success(`Created S3 access key ${next.access_key_id}`);
        await onSaved();
      }
    } catch (error) {
      message.error(normalizeApiError(error, editing ? 'Failed to update access key' : 'Failed to create S3 credential'));
    } finally {
      setSubmitting(false);
    }
  };

  const handleCopy = async (value: string, label: string) => {
    try {
      await copyText(value);
      message.success(`Copied ${label}`);
    } catch (error) {
      message.error(normalizeApiError(error, `Failed to copy ${label}`));
    }
  };

  return (
    <Modal
      confirmLoading={submitting}
      okText={created ? 'Done' : editing ? 'Save changes' : 'Create key'}
      onCancel={() => {
        if (submitting) {
          return;
        }
        onCancel();
      }}
      onOk={() => {
        if (created) {
          onCancel();
          return;
        }
        void handleSubmit();
      }}
      open={open}
      title={created ? 'S3 credential created' : editing ? `Edit ${credential?.access_key_id}` : 'Create S3 credential'}
    >
      {created ? (
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <div className="row-note">The secret key is only shown once. Copy it now.</div>
          <Descriptions
            column={1}
            items={[
              {
                key: 'access-key',
                label: 'Access key',
                children: (
                  <Space size={8} wrap>
                    <code>{created.access_key_id}</code>
                    <Button onClick={() => void handleCopy(created.access_key_id, 'access key')} size="small">
                      Copy
                    </Button>
                  </Space>
                ),
              },
              {
                key: 'secret-key',
                label: 'Secret key',
                children: (
                  <Space size={8} wrap>
                    <code>{created.secret_access_key}</code>
                    <Button onClick={() => void handleCopy(created.secret_access_key, 'secret key')} size="small" type="primary">
                      Copy
                    </Button>
                  </Space>
                ),
              },
              { key: 'label', label: 'Label', children: created.label || 'None' },
              { key: 'permission', label: 'Permission', children: s3CredentialPermissionLabel(created.permission) },
              { key: 'buckets', label: 'Buckets', children: s3CredentialBucketScopeLabel(created.buckets) },
              { key: 'created', label: 'Created', children: formatDateTime(created.created_at) },
            ]}
            size="small"
          />
        </Space>
      ) : (
        <Form form={form} initialValues={{ permission: 'read_write', buckets: [] }} layout="vertical">
          <Form.Item extra="Optional note so you remember which client owns this key." label="Label" name="label">
            <Input placeholder="CI runner" />
          </Form.Item>

          <Form.Item extra="Read only blocks uploads, deletes, and bucket mutations." label="Permission" name="permission">
            <Select options={s3CredentialPermissionOptions} />
          </Form.Item>

          <Form.Item extra="Leave empty for all buckets. You can pick existing buckets or type future names." label="Bucket scope" name="buckets">
            <Select mode="tags" options={bucketNames.map((name) => ({ label: name, value: name }))} tokenSeparators={[',']} />
          </Form.Item>
        </Form>
      )}
    </Modal>
  );
}
