import type { TransformationReport } from '../types/api'

interface Props {
  report: TransformationReport | null
  loading?: boolean
  error?: string | null
}

const SEVERITY_COLOR = {
  info: '#63b3ed',
  warn: '#f6ad55',
  error: '#fc8181',
} as const

const SEVERITY_LABEL = {
  info: 'INFO',
  warn: 'WARN',
  error: 'ERROR',
} as const

export function DiagnosticsPanel({ report, loading, error }: Props) {
  if (loading) {
    return (
      <div style={panelStyle}>
        <span style={{ color: '#a0aec0' }}>Translating…</span>
      </div>
    )
  }

  if (error) {
    return (
      <div style={panelStyle}>
        <span style={{ color: '#fc8181' }}>⚠ {error}</span>
      </div>
    )
  }

  if (!report) {
    return (
      <div style={panelStyle}>
        <span style={{ color: '#4a5568' }}>Paste T-SQL above and click Transform.</span>
      </div>
    )
  }

  const hasIssues = report.hard_errors.length > 0 || report.annotations.length > 0

  return (
    <div style={panelStyle}>
      {/* Summary bar */}
      <div style={summaryStyle}>
        <span style={{ color: report.success ? '#68d391' : '#fc8181' }}>
          {report.success ? '✓ Translation complete' : '✗ Translation has issues'}
        </span>
        <span style={{ color: '#a0aec0', marginLeft: 16 }}>
          {report.renames_applied > 0 && `${report.renames_applied} column(s) renamed · `}
          {report.udf_blocks_count > 0 && `${report.udf_blocks_count} UDF(s) generated · `}
          {report.used_llm && 'LLM used · '}
          {report.annotations.length} annotation(s)
        </span>
      </div>

      {/* Hard errors */}
      {report.hard_errors.map((err, i) => (
        <div key={i} style={{ ...rowStyle, borderLeft: '3px solid #fc8181' }}>
          <span style={{ color: '#fc8181', fontWeight: 600, marginRight: 8 }}>HARD ERROR</span>
          <span style={{ color: '#fed7d7' }}>{err}</span>
        </div>
      ))}

      {/* Annotations */}
      {report.annotations.map((ann, i) => (
        <div key={i} style={{ ...rowStyle, borderLeft: `3px solid ${SEVERITY_COLOR[ann.severity]}` }}>
          <span style={{ color: '#718096', width: 36, display: 'inline-block', flexShrink: 0 }}>
            {ann.line ?? '—'}
          </span>
          <span style={{
            color: SEVERITY_COLOR[ann.severity],
            width: 48,
            display: 'inline-block',
            fontWeight: 600,
            fontSize: 11,
            flexShrink: 0,
          }}>
            {SEVERITY_LABEL[ann.severity]}
          </span>
          <span style={{ color: '#e2e8f0', flex: 1 }}>{ann.message}</span>
        </div>
      ))}

      {!hasIssues && (
        <div style={{ color: '#4a5568', paddingTop: 4 }}>No issues detected.</div>
      )}
    </div>
  )
}

const panelStyle: React.CSSProperties = {
  padding: '12px 16px',
  background: '#161b27',
  fontFamily: 'monospace',
  fontSize: 12,
  overflowY: 'auto',
  height: '100%',
}

const summaryStyle: React.CSSProperties = {
  marginBottom: 8,
  paddingBottom: 8,
  borderBottom: '1px solid #2d3748',
  fontSize: 13,
}

const rowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'flex-start',
  gap: 8,
  padding: '4px 8px',
  marginBottom: 2,
  borderRadius: 2,
}
