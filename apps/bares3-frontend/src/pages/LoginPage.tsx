import { useState } from 'react';
import { CheckCircleFilled } from '@ant-design/icons';
import { Alert, Button, Form, Input, Space, Typography } from 'antd';
import { useLocation, useNavigate } from 'react-router-dom';
import { loginNotes } from '../console-data';
import { useAuth } from '../auth';
import { ThemeModeButton } from '../components/ThemeModeButton';
import { WaveBackground } from '../components/WaveBackground';
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
          <WaveBackground />
          <div className="brand-row brand-row-login" style={{ position: 'relative', zIndex: 10 }}>
            <img alt="BareS3 logo" className="brand-mark" src="/logo.png" />
            <div>
              <div className="brand-name">BareS3</div>
              <div className="brand-note">file-first object storage</div>
            </div>
          </div>

          <div className="login-hero-container" style={{ position: 'relative', zIndex: 10 }}>
            <div className="login-copy-block">
              <Title className="login-title" level={2}>
                Keep S3 outside. <br />
                Keep files readable inside.
              </Title>
              <Text className="login-note-text">
                Sign in with the console account configured on this node, then manage buckets and file routes in one place.
              </Text>
            </div>

            <div className="login-features">
              {loginNotes.map((item) => (
                <div className="login-feature-card" key={item.label}>
                  <CheckCircleFilled className="feature-icon" />
                  <div className="feature-text">
                    <strong>{item.label}</strong>
                    <span>{item.value}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </section>

        <section className="login-panel">
          <div className="login-panel-head">
            <div>
              <Title className="section-title login-panel-title" level={5}>
                Sign in
              </Title>
              <Text type="secondary">Login to BareS3 Console</Text>
            </div>
            <ThemeModeButton />
          </div>

          <Form
            className="login-form"
            initialValues={{ username: '', password: '' }}
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
                Login
              </Button>
            </Space>
          </Form>
        </section>
      </div>
    </div>
  );
}
