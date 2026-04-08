import { useState, useEffect } from 'react'
import { useSchema } from '../hooks/useSchema'

export function SchemaPanel() {
  const { status, mappings, loading, loadMappings, approveMapping } = useSchema()
  const [pendingOnly, setPendingOnly] = useState(false)
  const [editId, setEditId] = useState<number | null>(null)
  const [editValue, setEditValue] = useState('')

  useEffect(() => {
    loadMappings(pendingOnly)
  }, [pendingOnly, loadMappings])

  const handleApprove = async (id: number) => {
    const mapping = mappings.find(m => m.id === id)
    const value = editId === id ? editValue : (mapping?.tgt_column_name ?? '')
    if (!value) return
    await approveMapping(id, value)
    setEditId(null)
  }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: '#0f1117' }}>
      {/* Status bar */}
      {status && (
        <div style={statusBarStyle}>
          <span>Source tables: <b>{status.source_tables}</b></span>
          <span>Target tables: <b>{status.target_tables}</b></span>
          <span style={{ color: status.pending_mappings > 0 ? '#f6ad55' : '#68d391' }}>
            Pending review: <b>{status.pending_mappings}</b>
          </span>
          <span>Approved: <b>{status.approved_mappings}</b></span>
        </div>
      )}

      {/* Filter toggle */}
      <div style={{ padding: '8px 12px', borderBottom: '1px solid #2d3748', display: 'flex', gap: 12, alignItems: 'center' }}>
        <label style={{ fontSize: 12, color: '#a0aec0', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6 }}>
          <input
            type="checkbox"
            checked={pendingOnly}
            onChange={e => setPendingOnly(e.target.checked)}
          />
          Show pending only
        </label>
        <span style={{ fontSize: 12, color: '#4a5568' }}>{mappings.length} rows</span>
      </div>

      {/* Mapping table */}
      <div style={{ flex: 1, overflowY: 'auto' }}>
        {loading ? (
          <div style={{ padding: 16, color: '#4a5568' }}>Loading…</div>
        ) : mappings.length === 0 ? (
          <div style={{ padding: 16, color: '#4a5568' }}>
            No mappings found. Load DDL files with the CLI: <br />
            <code style={{ color: '#63b3ed' }}>tsql-migrator schema load-source --file schema.sql</code>
          </div>
        ) : (
          <table style={tableStyle}>
            <thead>
              <tr>
                {['Source Table', 'Source Column', 'Target Column', 'Confidence', 'Status', ''].map(h => (
                  <th key={h} style={thStyle}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {mappings.map(m => (
                <tr key={m.id} style={{ background: m.approved ? 'transparent' : '#1a1f2e' }}>
                  <td style={tdStyle}>{m.src_table_name}</td>
                  <td style={tdStyle}>{m.src_column_name}</td>
                  <td style={tdStyle}>
                    {editId === m.id ? (
                      <input
                        value={editValue}
                        onChange={e => setEditValue(e.target.value)}
                        style={inputStyle}
                        autoFocus
                        onKeyDown={e => { if (e.key === 'Enter') handleApprove(m.id) }}
                      />
                    ) : (
                      <span
                        style={{ color: m.tgt_column_name ? '#e2e8f0' : '#fc8181', cursor: 'pointer' }}
                        onClick={() => { setEditId(m.id); setEditValue(m.tgt_column_name ?? '') }}
                      >
                        {m.tgt_column_name ?? '⚠ unmapped'}
                      </span>
                    )}
                  </td>
                  <td style={{ ...tdStyle, color: m.confidence >= 0.9 ? '#68d391' : '#f6ad55' }}>
                    {(m.confidence * 100).toFixed(0)}%
                  </td>
                  <td style={tdStyle}>
                    {m.approved
                      ? <span style={{ color: '#68d391' }}>✓ approved</span>
                      : <span style={{ color: '#f6ad55' }}>pending</span>
                    }
                  </td>
                  <td style={tdStyle}>
                    {!m.approved && (
                      <button onClick={() => handleApprove(m.id)} style={btnStyle}>
                        Approve
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

const statusBarStyle: React.CSSProperties = {
  display: 'flex',
  gap: 20,
  padding: '8px 12px',
  fontSize: 12,
  color: '#a0aec0',
  borderBottom: '1px solid #2d3748',
  background: '#161b27',
}

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: 12,
  fontFamily: 'monospace',
}

const thStyle: React.CSSProperties = {
  padding: '6px 10px',
  textAlign: 'left',
  color: '#718096',
  borderBottom: '1px solid #2d3748',
  position: 'sticky',
  top: 0,
  background: '#0f1117',
  fontWeight: 600,
}

const tdStyle: React.CSSProperties = {
  padding: '5px 10px',
  borderBottom: '1px solid #1a202c',
  color: '#a0aec0',
}

const inputStyle: React.CSSProperties = {
  background: '#2d3748',
  border: '1px solid #4a5568',
  borderRadius: 3,
  color: '#e2e8f0',
  padding: '2px 6px',
  fontSize: 12,
  fontFamily: 'monospace',
  width: '100%',
}

const btnStyle: React.CSSProperties = {
  background: '#2b6cb0',
  border: 'none',
  borderRadius: 3,
  color: '#e2e8f0',
  padding: '2px 8px',
  fontSize: 11,
  cursor: 'pointer',
}
