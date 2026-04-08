import Editor, { type Monaco } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { useRef } from 'react'

interface SqlEditorProps {
  value: string
  onChange?: (value: string) => void
  language?: 'sql' | 'pgsql'
  readOnly?: boolean
  height?: string
  decorations?: Array<{ lineNumber: number; severity: 'warn' | 'error' }>
}

export function SqlEditor({
  value,
  onChange,
  language = 'sql',
  readOnly = false,
  height = '100%',
  decorations = [],
}: SqlEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const decorRef = useRef<string[]>([])

  function handleMount(editorInstance: editor.IStandaloneCodeEditor, monaco: Monaco) {
    editorRef.current = editorInstance

    // Apply decorations if any
    if (decorations.length > 0) {
      const newDecorations: editor.IModelDeltaDecoration[] = decorations.map(d => ({
        range: new monaco.Range(d.lineNumber, 1, d.lineNumber, 1),
        options: {
          isWholeLine: true,
          className: d.severity === 'error' ? 'line-error' : 'line-warn',
          glyphMarginClassName: d.severity === 'error' ? 'glyph-error' : 'glyph-warn',
        },
      }))
      decorRef.current = editorInstance.deltaDecorations(decorRef.current, newDecorations)
    }
  }

  return (
    <Editor
      height={height}
      language={language}
      value={value}
      onChange={v => onChange?.(v ?? '')}
      onMount={handleMount}
      theme="vs-dark"
      options={{
        readOnly,
        minimap: { enabled: false },
        fontSize: 13,
        lineNumbers: 'on',
        wordWrap: 'on',
        scrollBeyondLastLine: false,
        automaticLayout: true,
        padding: { top: 12, bottom: 12 },
        renderLineHighlight: readOnly ? 'none' : 'line',
        contextmenu: !readOnly,
      }}
    />
  )
}
