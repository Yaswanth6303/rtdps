"use client"

import type React from "react"

import { useRef, useState, useCallback } from "react"
import { UploadCloud, FileSpreadsheet, X, CheckCircle2, Loader2, AlertCircle } from "lucide-react"
import { Button } from "@/components/ui/button"

const ACCEPTED = ".csv"

function formatSize(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

type UploadPanelProps = {
  onUpload?: (file: File) => void | Promise<void>
  uploading?: boolean
  error?: string | null
}

export function UploadPanel({ onUpload, uploading = false, error = null }: UploadPanelProps) {
  const inputRef = useRef<HTMLInputElement>(null)
  const [dragging, setDragging] = useState(false)
  const [file, setFile] = useState<File | null>(null)

  const handleFiles = useCallback((files: FileList | null) => {
    if (files && files.length > 0) {
      setFile(files[0])
    }
  }, [])

  const onDrop = useCallback(
    (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault()
      setDragging(false)
      handleFiles(e.dataTransfer.files)
    },
    [handleFiles],
  )

  const submit = async () => {
    if (!file) return
    await onUpload?.(file)
    setFile(null)
    if (inputRef.current) inputRef.current.value = ""
  }

  return (
    <section
      aria-labelledby="upload-heading"
      className="rounded-xl border border-border bg-card p-6 shadow-sm"
    >
      <div className="mb-4 flex items-center gap-2">
        <FileSpreadsheet className="h-5 w-5 text-primary" aria-hidden="true" />
        <h2 id="upload-heading" className="text-base font-semibold text-card-foreground">
          Upload CSV File
        </h2>
      </div>

      <div
        role="button"
        tabIndex={0}
        aria-label="Upload a CSV file by clicking or dragging it here"
        onClick={() => inputRef.current?.click()}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault()
            inputRef.current?.click()
          }
        }}
        onDragOver={(e) => {
          e.preventDefault()
          setDragging(true)
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        className={`flex cursor-pointer flex-col items-center justify-center gap-3 rounded-lg border-2 border-dashed px-6 py-10 text-center transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
          dragging ? "border-primary bg-accent" : "border-border bg-muted/40 hover:bg-muted"
        }`}
      >
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-accent">
          <UploadCloud className="h-6 w-6 text-primary" aria-hidden="true" />
        </div>
        <div className="space-y-1">
          <p className="text-sm font-medium text-foreground">
            Drag &amp; drop your CSV file here
          </p>
          <p className="text-xs text-muted-foreground">
            or click to browse — supports {".csv"} only
          </p>
        </div>
        <input
          ref={inputRef}
          type="file"
          accept={ACCEPTED}
          className="sr-only"
          onChange={(e) => handleFiles(e.target.files)}
        />
      </div>

      {file && (
        <div className="mt-4 flex items-center justify-between gap-3 rounded-lg border border-border bg-muted/40 px-4 py-3">
          <div className="flex min-w-0 items-center gap-3">
            <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-md bg-accent">
              <FileSpreadsheet className="h-4 w-4 text-primary" aria-hidden="true" />
            </div>
            <div className="min-w-0">
              <p className="truncate text-sm font-medium text-foreground">{file.name}</p>
              <p className="text-xs text-muted-foreground">{formatSize(file.size)}</p>
            </div>
          </div>
          <button
            type="button"
            aria-label="Remove selected file"
            disabled={uploading}
            onClick={() => {
              setFile(null)
              if (inputRef.current) inputRef.current.value = ""
            }}
            className="rounded-md p-1 text-muted-foreground transition-colors hover:bg-background hover:text-foreground disabled:opacity-50"
          >
            <X className="h-4 w-4" aria-hidden="true" />
          </button>
        </div>
      )}

      {error && (
        <div className="mt-4 flex items-start gap-2 rounded-lg border border-destructive/20 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          <AlertCircle className="h-4 w-4 shrink-0" aria-hidden="true" />
          <span className="break-words">{error}</span>
        </div>
      )}

      <div className="mt-4 flex items-center justify-end gap-3">
        {file && !uploading && (
          <span className="mr-auto flex items-center gap-1.5 text-xs text-muted-foreground">
            <CheckCircle2 className="h-3.5 w-3.5 text-primary" aria-hidden="true" />
            Ready to process
          </span>
        )}
        {uploading && (
          <span className="mr-auto flex items-center gap-1.5 text-xs text-muted-foreground">
            <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" aria-hidden="true" />
            Uploading…
          </span>
        )}
        <Button disabled={!file || uploading} onClick={submit}>
          {uploading ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
              Uploading…
            </>
          ) : (
            <>
              <UploadCloud className="h-4 w-4" aria-hidden="true" />
              Upload &amp; Process
            </>
          )}
        </Button>
      </div>
    </section>
  )
}
