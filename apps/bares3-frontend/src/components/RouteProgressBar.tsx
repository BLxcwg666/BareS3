import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';

export function RouteProgressBar() {
  const location = useLocation();
  const [state, setState] = useState<'idle' | 'loading' | 'done'>('idle');

  useEffect(() => {
    setState('loading');

    const complete = window.setTimeout(() => {
      setState('done');
    }, 140);

    const reset = window.setTimeout(() => {
      setState('idle');
    }, 320);

    return () => {
      window.clearTimeout(complete);
      window.clearTimeout(reset);
    };
  }, [location.pathname]);

  return <div aria-hidden className={`route-progress route-progress-${state}`} />;
}
