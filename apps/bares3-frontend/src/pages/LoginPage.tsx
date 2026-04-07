import { useState } from 'react';
import { Alert, Button, Form, Input, Space, Typography } from 'antd';
import { useLocation, useNavigate } from 'react-router-dom';
import { loginNotes } from '../console-data';
import { useAuth } from '../auth';
import { ThemeModeButton } from '../components/ThemeModeButton';
import { normalizeApiError } from '../utils';

const { Text, Title } = Typography;

export function LoginPage() {
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const redirectTarget = typeof location.state?.from === 'string' ? location.state.from : '/overview';

  const handleSubmit = async (values: { username: string; password: string }) => {
    setSubmitting(true);
    setError(null);
    try {
      await auth.login(values.username.trim(), values.password);
      navigate(redirectTarget, { replace: true });
    } catch (nextError) {
      setError(normalizeApiError(nextError, 'Failed to sign in.'));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="login-page">
      <div className="login-frame route-fade">
        <section className="login-aside">
          <div className="brand-row brand-row-login">
            <img alt="BareS3 logo" className="brand-mark" src="/logo.png" />
            <div>
              <div className="brand-name">BareS3</div>
              <div className="brand-note">file-first object storage</div>
            </div>
          </div>

          <div className="login-copy-block">
            <Title className="login-title" level={2}>
              Keep S3 outside. Keep files readable inside.
            </Title>
            <Text className="login-note-text">
              Sign in with the console account configured on this node, then manage buckets and file routes in one place.
            </Text>
          </div>

          <div className="login-lines">
            {loginNotes.map((item) => (
              <div className="login-line" key={item.label}>
                <span>{item.label}</span>
                <strong>{item.value}</strong>
              </div>
            ))}
          </div>
        </section>

        <section className="login-panel">
          <div className="login-panel-head">
            <div>
              <Title className="section-title" level={5}>
                Sign in
              </Title>
              <Text type="secondary">The console uses one configured admin account and a signed session cookie.</Text>
            </div>
            <ThemeModeButton />
          </div>

          <Form
            className="login-form"
            initialValues={{ username: 'admin', password: '' }}
            layout="vertical"
            onFinish={handleSubmit}
          >
            <Form.Item label="Username" name="username" rules={[{ required: true, message: 'Username is required' }]}>
              <Input autoComplete="username" />
            </Form.Item>
            <Form.Item label="Password" name="password" rules={[{ required: true, message: 'Password is required' }]}>
              <Input.Password autoComplete="current-password" />
            </Form.Item>

            {error ? <Alert className="login-alert" message={error} type="error" showIcon /> : null}

            <Space className="login-actions" size={8} wrap>
              <Button htmlType="submit" loading={submitting} type="primary">
                Enter console
              </Button>
            </Space>
          </Form>
        </section>
      </div>
    </div>
  );
}
