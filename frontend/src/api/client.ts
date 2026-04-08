import type { HistoryItem, MappingItem, SchemaStatus, TranslateRequest, TranslateResponse } from '../types/api'

const BASE = '/api'

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`${res.status}: ${text}`)
  }
  return res.json()
}

async function get<T>(path: string, params?: Record<string, string | number | boolean>): Promise<T> {
  const url = new URL(`${BASE}${path}`, window.location.origin)
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, String(v)))
  }
  const res = await fetch(url.toString())
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`)
  return res.json()
}

async function patch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`)
  return res.json()
}

export const api = {
  translate: (req: TranslateRequest) => post<TranslateResponse>('/translate', req),
  schema: {
    status: () => get<SchemaStatus>('/schema/status'),
    mappings: (pendingOnly = false) =>
      get<MappingItem[]>('/schema/mappings', { pending_only: pendingOnly }),
    updateMapping: (id: number, body: { tgt_column_name?: string | null; approved?: boolean; notes?: string }) =>
      patch<MappingItem>(`/schema/mappings/${id}`, body),
    runDiff: () => post<{ total: number; approved: number; pending: number }>('/schema/diff', {}),
  },
  history: (limit = 20) => get<HistoryItem[]>('/history', { limit }),
}
