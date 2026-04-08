import { useState, useRef } from 'react'
import { SqlEditor } from './SqlEditor'
import { DiagnosticsPanel } from './DiagnosticsPanel'
import { useTranslate } from '../hooks/useTranslate'
import type { AnnotationItem } from '../types/api'

const PLACEHOLDER = `-- Paste your T-SQL query here
SELECT TOP 100
    o.CustomerID,
    o.OrderDate,
    ISNULL(t.TerritoryName, 'Unknown') AS TerritoryName
FROM dbo.SalesOrderHeader o WITH (NOLOCK)
LEFT JOIN dbo.SalesTerritory t WITH (NOLOCK)
    ON o.TerritoryID = t.TerritoryID
WHERE o.OrderDate >= DATEADD(DAY, -90, GETDATE())
  AND o.Status = 1
ORDER BY o.OrderDate DESC;`

export function TranslatePanel() {
  const [inputSql, setInputSql] = useState('')
  const [schemaName, setSchemaName] = useState('')
  const { outputSql, report, loading, error, translate } = useTranslate()

  const handleTranslate = () => {
    translate(inputSql || PLACEHOLDER, schemaName || undefined)
  }

  const handleCopy = () => {
    if (outputSql) navigator.clipboard.writeText(outputSql)
  }

  // Build line decorations for the output editor from annotations
  const decorations = (report?.annotations ?? [])
    .filter((a): a is AnnotationItem & { line: number } => a.line !== null)
    .map(a => ({ lineNumber: a.line, severity: a.severity === 'error' ? 'error' as const : 'warn' as const }))

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Toolbar */}
      <div style={toolbarStyle}>
        <span style={{ color: '#63b3ed', fontWeight: 700, fontSize: 15 }}>
          T-SQL → Redshift
        </span>

        <input
          placeholder="Schema name (optional)"
          value={schemaName}
          onChange={e => setSchemaName(e.target.value)}
          style={schemaInputStyle}
        />

        <button onClick={handleTranslate} disabled={loading} style={primaryBtnStyle}>
          {loading ? 'Translating…' : '⚡ Transform'}
        </button>

        {outputSql && (
          <button onClick={handleCopy} style={secondaryBtnStyle}>
            Copy
          </button>
        )}

        {report && (
          <span style={{
            fontSize: 12, marginLeft: 8,
            color: report.success ? '#68d391' : '#fc8181',
          }}>
            {report.success ? '✓ success' : '✗ issues'}
          </span>
        )}
      </div>

      {/* Editor panes */}
      <div style={{ flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', minHeight: 0 }}>
        {/* Input */}
        <div style={paneStyle}>
          <div style={paneLabelStyle}>INPUT — T-SQL</div>
          <div style={{ flex: 1, minHeight: 0 }}>
            <SqlEditor
              value={inputSql}
              onChange={setInputSql}
              language="sql"
            />
          </div>
        </div>

        {/* Output */}
        <div style={{ ...paneStyle, borderLeft: '1px solid #2d3748' }}>
          <div style={paneLabelStyle}>OUTPUT — Redshift SQL</div>
          <div style={{ flex: 1, minHeight: 0 }}>
            <SqlEditor
              value={outputSql || (loading ? '-- translating…' : '-- output will appear here')}
              language="pgsql"
              readOnly
              decorations={decorations}
            />
          </div>
        </div>
      </div>

      {/* Diagnostics panel */}
      <div style={diagnosticsContainerStyle}>
        <DiagnosticsPanel report={report} loading={loading} error={error} />
      </div>
    </div>
  )
}

const toolbarStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 10,
  padding: '10px 16px',
  background: '#161b27',
  borderBottom: '1px solid #2d3748',
  flexShrink: 0,
}

const paneStyle: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  minHeight: 0,
  overflow: 'hidden',
}

const paneLabelStyle: React.CSSProperties = {
  padding: '4px 12px',
  fontSize: 11,
  color: '#4a5568',
  background: '#0f1117',
  borderBottom: '1px solid #1a202c',
  letterSpacing: '0.08em',
  fontWeight: 600,
  flexShrink: 0,
}

const diagnosticsContainerStyle: React.CSSProperties = {
  height: 180,
  borderTop: '1px solid #2d3748',
  flexShrink: 0,
  overflow: 'hidden',
}

const schemaInputStyle: React.CSSProperties = {
  background: '#2d3748',
  border: '1px solid #4a5568',
  borderRadius: 4,
  color: '#e2e8f0',
  padding: '4px 10px',
  fontSize: 13,
  width: 200,
}

const primaryBtnStyle: React.CSSProperties = {
  background: '#2b6cb0',
  border: 'none',
  borderRadius: 4,
  color: '#e2e8f0',
  padding: '6px 16px',
  fontSize: 13,
  fontWeight: 600,
  cursor: 'pointer',
}

const secondaryBtnStyle: React.CSSProperties = {
  background: '#2d3748',
  border: '1px solid #4a5568',
  borderRadius: 4,
  color: '#a0aec0',
  padding: '6px 12px',
  fontSize: 13,
  cursor: 'pointer',
}
