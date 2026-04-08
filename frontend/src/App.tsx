import { useState } from 'react'
import { TranslatePanel } from './components/TranslatePanel'
import { SchemaPanel } from './components/SchemaPanel'

type Tab = 'translate' | 'schema'

export default function App() {
  const [tab, setTab] = useState<Tab>('translate')

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', background: '#0f1117' }}>
      {/* Nav */}
      <nav style={navStyle}>
        <span style={{ color: '#63b3ed', fontWeight: 700, fontSize: 14, letterSpacing: '0.05em', marginRight: 24 }}>
          tsql-migrator
        </span>
        {(['translate', 'schema'] as Tab[]).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              ...tabBtnStyle,
              color: tab === t ? '#e2e8f0' : '#718096',
              borderBottom: tab === t ? '2px solid #63b3ed' : '2px solid transparent',
            }}
          >
            {t === 'translate' ? '⚡ Translate' : '🗂 Schema Mappings'}
          </button>
        ))}
      </nav>

      {/* Content */}
      <main style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}>
        {tab === 'translate' ? <TranslatePanel /> : <SchemaPanel />}
      </main>
    </div>
  )
}

const navStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  padding: '0 16px',
  background: '#161b27',
  borderBottom: '1px solid #2d3748',
  height: 44,
  flexShrink: 0,
}

const tabBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  padding: '0 12px',
  height: 44,
  fontSize: 13,
  fontWeight: 500,
}
