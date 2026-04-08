import { useState, useCallback } from 'react'
import { api } from '../api/client'
import type { TransformationReport } from '../types/api'

interface TranslateState {
  outputSql: string
  report: TransformationReport | null
  loading: boolean
  error: string | null
}

export function useTranslate() {
  const [state, setState] = useState<TranslateState>({
    outputSql: '',
    report: null,
    loading: false,
    error: null,
  })

  const translate = useCallback(async (sql: string, schemaName?: string) => {
    if (!sql.trim()) return
    setState(s => ({ ...s, loading: true, error: null }))
    try {
      const result = await api.translate({ sql, schema_name: schemaName || undefined })
      setState({ outputSql: result.output_sql, report: result.report, loading: false, error: null })
    } catch (e) {
      setState(s => ({ ...s, loading: false, error: String(e) }))
    }
  }, [])

  return { ...state, translate }
}
