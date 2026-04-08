export type Severity = 'info' | 'warn' | 'error'

export interface AnnotationItem {
  line: number | null
  message: string
  severity: Severity
}

export interface TransformationReport {
  success: boolean
  annotations: AnnotationItem[]
  hard_errors: string[]
  renames_applied: number
  udf_blocks_count: number
  used_llm: boolean
  llm_confidence: number | null
}

export interface TranslateRequest {
  sql: string
  schema_name?: string
  enable_llm?: boolean
  target_context?: 'query-editor' | 'quicksight' | 'power-bi' | 'tableau'
}

export interface TranslateResponse {
  output_sql: string
  report: TransformationReport
}

export interface MappingItem {
  id: number
  src_table_schema: string
  src_table_name: string
  src_column_name: string
  tgt_table_schema: string
  tgt_table_name: string
  tgt_column_name: string | null
  confidence: number
  source: string
  approved: boolean
  notes: string | null
}

export interface SchemaStatus {
  source_tables: number
  target_tables: number
  total_mappings: number
  approved_mappings: number
  pending_mappings: number
  unmapped_columns: number
}

export interface TableItem {
  schema_name: string
  table_name: string
}

export interface SourceTableItem {
  schema_name: string
  table_name: string
  mapped: boolean
  tgt_schema: string | null
  tgt_table: string | null
}

export interface TableMappingItem {
  id: number
  src_table_schema: string
  src_table_name: string
  tgt_table_schema: string
  tgt_table_name: string
  confidence: number
  source: string
  approved: boolean
}

export interface HistoryItem {
  id: number
  input_sql: string
  output_sql: string
  used_llm: boolean
  created_at: string
}
