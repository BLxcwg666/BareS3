import { useEffect, useState } from 'react';
import { App as AntApp, Form, Input, InputNumber, Modal, Select, Space, Switch } from 'antd';
import { createBucket } from '../api';
import { bucketAccessModeOptions, sizeUnitOptions } from '../constants';
import type { BucketCreateValues } from '../types';
import { normalizeApiError, sizeInputToBytes } from '../utils';

export function BucketCreateModal({
  open,
  onCancel,
  onCreated,
}: {
  open: boolean;
  onCancel: () => void;
  onCreated: () => Promise<void> | void;
}) {
  const { message } = AntApp.useApp();
  const [form] = Form.useForm<BucketCreateValues>();
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) {
      return;
    }

    form.setFieldsValue({
      name: '',
      accessMode: 'private',
      replicationEnabled: false,
      quotaValue: undefined,
      quotaUnit: 'GB',
    });
  }, [form, open]);

  const handleSubmit = async () => {
    const values = await form.validateFields();
    setSubmitting(true);
    try {
      const bucket = await createBucket(values.name.trim(), sizeInputToBytes(values.quotaValue, values.quotaUnit), values.accessMode, values.replicationEnabled);
      message.success(`Bucket ${bucket.name} created`);
      form.resetFields();
      onCancel();
      await onCreated();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to create bucket'));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      confirmLoading={submitting}
      okText="Create bucket"
      onCancel={() => {
        if (submitting) {
          return;
        }
        form.resetFields();
        onCancel();
      }}
      onOk={() => void handleSubmit()}
      open={open}
      title="New bucket"
    >
      <Form form={form} initialValues={{ accessMode: 'private', replicationEnabled: false, quotaUnit: 'GB' }} layout="vertical">
        <Form.Item label="Bucket name" name="name" rules={[{ required: true, whitespace: true, message: 'Bucket name is required' }]}>
          <Input placeholder="gallery" />
        </Form.Item>

        <Form.Item
          extra="Private requires auth, Public enables /pub, and Custom uses the access rules page."
          label="Access mode"
          name="accessMode"
          rules={[{ required: true, message: 'Access mode is required' }]}
        >
          <Select options={bucketAccessModeOptions} />
        </Form.Item>

        <Form.Item extra="New buckets stay out of replication until you turn this on." label="Replication" name="replicationEnabled" valuePropName="checked">
          <Switch checkedChildren="On" unCheckedChildren="Off" />
        </Form.Item>

        <Form.Item extra="Leave empty or 0 for unlimited." label="Bucket limit">
          <Space.Compact block>
            <Form.Item name="quotaValue" noStyle>
              <InputNumber min={0} placeholder="Unlimited" precision={3} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="quotaUnit" noStyle>
              <Select
                options={sizeUnitOptions.map((option) => ({ label: option.label, value: option.value }))}
                style={{ width: 96 }}
              />
            </Form.Item>
          </Space.Compact>
        </Form.Item>
      </Form>
    </Modal>
  );
}
