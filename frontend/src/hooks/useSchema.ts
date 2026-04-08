import { useState, useEffect, useCallback } from 'react'
import { api } from '../api/client'
import type { MappingItem, SchemaStatus } from '../types/api'

export function useSchema() {
  const [status, setStatus] = useState<SchemaStatus | null>(null)
  const [mappings, setMappings] = useState<MappingItem[]>([])
  const [loading, setLoading] = useState(false)

  const loadStatus = useCallback(async () => {
    try {
      const s = await api.schema.status()
      setStatus(s)
    } catch { /* ignore */ }
  }, [])

  const loadMappings = useCallback(async (pendingOnly = false) => {
    setLoading(true)
    try {
      const m = await api.schema.mappings(pendingOnly)
      setMappings(m)
    } finally {
      setLoading(false)
    }
  }, [])

  const approveMapping = useCallback(async (id: number, tgtCol: string) => {
    await api.schema.updateMapping(id, { tgt_column_name: tgtCol, approved: true })
    setMappings(prev => prev.map(m => m.id === id ? { ...m, tgt_column_name: tgtCol, approved: true } : m))
  }, [])

  useEffect(() => { loadStatus() }, [loadStatus])

  return { status, mappings, loading, loadMappings, approveMapping, refresh: loadStatus }
}
