import { useState, useEffect, useCallback } from 'react'
import { api } from '../api/client'
import type { SourceTableItem, TableItem } from '../types/api'

export function TableMappingsSection() {
  const [sourceTables, setSourceTables] = useState<SourceTableItem[]>([])
  const [targetTables, setTargetTables] = useState<TableItem[]>([])
  const [loading, setLoading] = useState(false)
  const [unmappedOnly, setUnmappedOnly] = useState(false)
  const [filter, setFilter] = useState('')
  // Per-row pending selection: key = "schema.table"
  const [selections, setSelections] = useState<Record<string, string>>({})
  const [saving, setSaving] = useState<Record<string, boolean>>({})
  const [errors, setErrors] = useState<Record<string, string>>({})

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const [src, tgt] = await Promise.all([
        api.schema.sourceTables(),
        api.schema.targetTables(),
      ])
      setSourceTables(src)
      setTargetTables(tgt)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  const rowKey = (t: SourceTableItem) => `${t.schema_name}.${t.table_name}`

  const currentSelection = (t: SourceTableItem) =>
    selections[rowKey(t)] ?? (t.tgt_schema && t.tgt_table ? `${t.tgt_schema}.${t.tgt_table}` : '')

  const handleSave = async (t: SourceTableItem) => {
    const val = currentSelection(t).trim()
    const parts = val.split('.')
    if (parts.length < 2 || !parts[1]) {
      setErrors(prev => ({ ...prev, [rowKey(t)]: 'Use format schema.table_name' }))
      return
    }
    const [tgtSchema, ...rest] = parts
    const tgtTable = rest.join('.')
    setSaving(prev => ({ ...prev, [rowKey(t)]: true }))
    setErrors(prev => ({ ...prev, [rowKey(t)]: '' }))
    try {
      await api.schema.saveTableMapping({
        src_schema: t.schema_name,
        src_table: t.table_name,
        tgt_schema: tgtSchema,
        tgt_table: tgtTable,
      })
      setSourceTables(prev => prev.map(st =>
        rowKey(st) === rowKey(t)
          ? { ...st, mapped: true, tgt_schema: tgtSchema, tgt_table: tgtTable }
          : st
      ))
      setSelections(prev => ({ ...prev, [rowKey(t)]: val }))
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e)
      setErrors(prev => ({ ...prev, [rowKey(t)]: msg }))
    } finally {
      setSaving(prev => ({ ...prev, [rowKey(t)]: false }))
    }
  }

  const datalistId = 'target-tables-list'
  const filterLower = filter.toLowerCase()
  const visible = sourceTables.filter(t => {
    if (unmappedOnly && t.mapped) return false
    if (filter && !`${t.schema_name}.${t.table_name}`.toLowerCase().includes(filterLower)) return false
    return true
  })

  const unmappedCount = sourceTables.filter(t => !t.mapped).length

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* datalist for autocomplete */}
      <datalist id={datalistId}>
        {targetTables.map(t => (
          <option key={`${t.schema_name}.${t.table_name}`} value={`${t.schema_name}.${t.table_name}`} />
        ))}
      </datalist>

      {/* Toolbar */}
      <div style={toolbarStyle}>
        <label style={checkboxLabelStyle}>
          <input
            type="checkbox"
            checked={unmappedOnly}
            onChange={e => setUnmappedOnly(e.target.checked)}
          />
          Unmapped only
        </label>
        <input
          type="text"
          placeholder="Filter source tables…"
          value={filter}
          onChange={e => setFilter(e.target.value)}
          style={filterInputStyle}
        />
        <span style={{ fontSize: 12, color: '#4a5568', marginLeft: 'auto' }}>
          {unmappedCount > 0 && (
            <span style={{ color: '#f6ad55', marginRight: 12 }}>{unmappedCount} unmapped</span>
          )}
          {visible.length} / {sourceTables.length} rows
        </span>
        <button onClick={load} style={refreshBtnStyle} title="Reload">↺</button>
      </div>

      {/* Table */}
      <div style={{ flex: 1, overflowY: 'auto' }}>
        {loading ? (
          <div style={{ padding: 16, color: '#4a5568' }}>Loading…</div>
        ) : sourceTables.length === 0 ? (
          <div style={{ padding: 16, color: '#4a5568' }}>
            No source tables loaded. Run <code style={{ color: '#63b3ed' }}>tsql-migrator schema load-source</code> first.
          </div>
        ) : (
          <table style={tableStyle}>
            <thead>
              <tr>
                {['Source Table', 'Target Table', 'Status', ''].map(h => (
                  <th key={h} style={thStyle}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {visible.map(t => {
                const key = rowKey(t)
                const sel = currentSelection(t)
                const isSaving = saving[key]
                const err = errors[key]
                return (
                  <tr key={key} style={{ background: t.mapped ? 'transparent' : '#1a1f2e' }}>
                    <td style={{ ...tdStyle, fontFamily: 'monospace', color: '#e2e8f0' }}>
                      {t.schema_name}.{t.table_name}
                    </td>
                    <td style={{ ...tdStyle, minWidth: 260 }}>
                      <input
                        list={datalistId}
                        value={sel}
                        onChange={e => setSelections(prev => ({ ...prev, [key]: e.target.value }))}
                        onKeyDown={e => { if (e.key === 'Enter') handleSave(t) }}
                        placeholder="schema.table_name"
                        style={{
                          ...targetInputStyle,
                          borderColor: err ? '#fc8181' : '#2d3748',
                        }}
                      />
                      {err && <div style={{ color: '#fc8181', fontSize: 10, marginTop: 2 }}>{err}</div>}
                    </td>
                    <td style={tdStyle}>
                      {t.mapped
                        ? <span style={{ color: '#68d391' }}>✓ mapped</span>
                        : <span style={{ color: '#f6ad55' }}>⚠ unmapped</span>
                      }
                    </td>
                    <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>
                      <button
                        onClick={() => handleSave(t)}
                        disabled={isSaving || !sel.trim()}
                        style={{
                          ...saveBtnStyle,
                          opacity: isSaving || !sel.trim() ? 0.4 : 1,
                          cursor: isSaving || !sel.trim() ? 'default' : 'pointer',
                        }}
                      >
                        {isSaving ? '…' : t.mapped ? 'Re-map' : 'Map'}
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

const toolbarStyle: React.CSSProperties = {
  display: 'flex',
  gap: 12,
  alignItems: 'center',
  padding: '8px 12px',
  borderBottom: '1px solid #2d3748',
  flexShrink: 0,
}

const checkboxLabelStyle: React.CSSProperties = {
  fontSize: 12,
  color: '#a0aec0',
  cursor: 'pointer',
  display: 'flex',
  alignItems: 'center',
  gap: 6,
  whiteSpace: 'nowrap',
}

const filterInputStyle: React.CSSProperties = {
  background: '#1a202c',
  border: '1px solid #2d3748',
  borderRadius: 3,
  color: '#e2e8f0',
  padding: '3px 8px',
  fontSize: 12,
  width: 200,
}

const refreshBtnStyle: React.CSSProperties = {
  background: 'none',
  border: '1px solid #2d3748',
  borderRadius: 3,
  color: '#718096',
  padding: '2px 8px',
  fontSize: 14,
  cursor: 'pointer',
}

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: 12,
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

const targetInputStyle: React.CSSProperties = {
  background: '#2d3748',
  border: '1px solid #2d3748',
  borderRadius: 3,
  color: '#e2e8f0',
  padding: '3px 8px',
  fontSize: 12,
  fontFamily: 'monospace',
  width: '100%',
}

const saveBtnStyle: React.CSSProperties = {
  background: '#2b6cb0',
  border: 'none',
  borderRadius: 3,
  color: '#e2e8f0',
  padding: '2px 10px',
  fontSize: 11,
}
