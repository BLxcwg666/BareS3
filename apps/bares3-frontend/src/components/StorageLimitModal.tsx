import { useEffect, useState } from 'react';
import { App as AntApp, Form, InputNumber, Modal, Select, Space } from 'antd';
import { updateStorageLimit } from '../api';
import { sizeUnitOptions } from '../constants';
import type { StorageLimitValues } from '../types';
import { bytesToSizeInput, normalizeApiError, sizeInputToBytes } from '../utils';

export function StorageLimitModal({
  currentMaxBytes,
  open,
  onCancel,
  onSaved,
}: {
  currentMaxBytes: number;
  open: boolean;
  onCancel: () => void;
  onSaved: () => Promise<void> | void;
}) {
  const { message } = AntApp.useApp();
  const [form] = Form.useForm<StorageLimitValues>();
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) {
      return;
    }

    const nextValue = bytesToSizeInput(currentMaxBytes);
    form.setFieldsValue({
      maxValue: nextValue.value,
      maxUnit: nextValue.unit,
    });
  }, [currentMaxBytes, form, open]);

  const handleSubmit = async () => {
    const values = await form.validateFields();
    setSubmitting(true);
    try {
      await updateStorageLimit(sizeInputToBytes(values.maxValue, values.maxUnit));
      message.success('Storage limit updated');
      onCancel();
      await onSaved();
    } catch (error) {
      message.error(normalizeApiError(error, 'Failed to update storage limit'));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      confirmLoading={submitting}
      okText="Save limit"
      onCancel={() => {
        if (submitting) {
          return;
        }
        onCancel();
      }}
      onOk={() => void handleSubmit()}
      open={open}
      title="Instance storage limit"
    >
      <Form form={form} initialValues={{ maxUnit: 'GB' }} layout="vertical">
        <Form.Item extra="Leave empty or 0 for unlimited." label="Maximum storage">
          <Space.Compact block>
            <Form.Item name="maxValue" noStyle>
              <InputNumber min={0} placeholder="Unlimited" precision={1} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="maxUnit" noStyle>
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
