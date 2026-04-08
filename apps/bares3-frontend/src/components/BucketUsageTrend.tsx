import type { BucketUsageSample } from '../api';
import { formatBytes, formatRelativeTime } from '../utils';

const chartWidth = 320;
const chartHeight = 72;
const chartPadding = 8;

function chartPath(points: Array<{ x: number; y: number }>) {
  return points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x} ${point.y}`).join(' ');
}

export function BucketUsageTrend({
  points,
}: {
  points: BucketUsageSample[];
}) {
  if (points.length === 0) {
    return <div className="row-note">Usage snapshots appear after uploads, deletes, moves, or quota changes.</div>;
  }

  const samples = points.length === 1 ? [points[0], points[0]] : points;
  const maxUsedBytes = Math.max(...samples.map((item) => item.used_bytes), 0);
  const chartCeiling = maxUsedBytes <= 0 ? 1 : maxUsedBytes * 1.15;
  const drawableWidth = chartWidth - chartPadding * 2;
  const drawableHeight = chartHeight - chartPadding * 2;
  const step = samples.length > 1 ? drawableWidth / (samples.length - 1) : 0;
  const coords = samples.map((item, index) => ({
    x: chartPadding + step * index,
    y: chartHeight - chartPadding - (item.used_bytes / chartCeiling) * drawableHeight,
  }));
  const line = chartPath(coords);
  const area = `${line} L ${coords[coords.length - 1].x} ${chartHeight - chartPadding} L ${coords[0].x} ${chartHeight - chartPadding} Z`;
  const baselineY = chartHeight - chartPadding;
  const earliest = points[0];
  const latest = points[points.length - 1];

  return (
    <div className="bucket-trend">
      <div className="bucket-trend-stats">
        <div className="bucket-trend-stat">
          <div className="path-label">Current</div>
          <div className="row-title">{formatBytes(latest.used_bytes)}</div>
        </div>
        <div className="bucket-trend-stat">
          <div className="path-label">Peak</div>
          <div className="row-title">{formatBytes(maxUsedBytes)}</div>
        </div>
        <div className="bucket-trend-stat bucket-trend-stat-end">
          <div className="path-label">Latest</div>
          <div className="row-title">{formatRelativeTime(latest.recorded_at)}</div>
        </div>
      </div>

      <svg aria-hidden="true" className="bucket-trend-svg" viewBox={`0 0 ${chartWidth} ${chartHeight}`}>
        {[0.3, 0.6].map((ratio) => {
          const y = chartPadding + drawableHeight * ratio;
          return <line className="bucket-trend-grid" key={ratio} x1={chartPadding} x2={chartWidth - chartPadding} y1={y} y2={y} />;
        })}
        <line className="bucket-trend-axis" x1={chartPadding} x2={chartWidth - chartPadding} y1={baselineY} y2={baselineY} />
        <path className="bucket-trend-area" d={area} />
        <path className="bucket-trend-line" d={line} />
        <circle className="bucket-trend-dot" cx={coords[coords.length - 1].x} cy={coords[coords.length - 1].y} r="3" />
      </svg>

      <div className="bucket-trend-foot">
        <span>{formatRelativeTime(earliest.recorded_at)}</span>
        <span>{points.length} snapshot{points.length === 1 ? '' : 's'}</span>
      </div>
    </div>
  );
}
