"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { Activity, Database } from "lucide-react"
import { UploadPanel } from "@/components/upload-panel"
import { RecentJobs, type Job, type JobStatus } from "@/components/recent-jobs"
import { ThemeToggle } from "@/components/theme-toggle"

// ------------------------------ Backend mapping -----------------------------

// Shape returned by GET /jobs/all and /jobs/status/{id} — mirrors JobTracker.java.
type JobTracker = {
  id: string
  jobType: string
  filePath: string | null
  status: string // QUEUED | PROCESSING | COMPLETED | FAILED | STARTING | STOPPING | ...
  executionId: number | null
  readCount: number
  writeCount: number
  skipCount: number
  durationMs: number
  errorMessage: string | null
  createdAt: string
  updatedAt: string | null
}

function statusFromBackend(raw: string): JobStatus {
  switch (raw) {
    case "COMPLETED":
      return "completed"
    case "FAILED":
    case "ABANDONED":
    case "STOPPED":
      return "failed"
    case "QUEUED":
      return "queued"
    default: // PROCESSING, STARTING, STARTED, STOPPING, UNKNOWN
      return "processing"
  }
}

function relativeTime(iso: string): string {
  const t = new Date(iso).getTime()
  const diff = Date.now() - t
  const sec = Math.max(1, Math.floor(diff / 1000))
  if (sec < 60) return "Just now"
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min} min ago`
  const hr = Math.floor(min / 60)
  if (hr < 24) return `${hr} hour${hr === 1 ? "" : "s"} ago`
  const day = Math.floor(hr / 24)
  return `${day} day${day === 1 ? "" : "s"} ago`
}

function trackerToJob(t: JobTracker): Job {
  return {
    id: t.id,
    type: t.jobType === "IMPORT" ? "CSV Import" : t.jobType,
    status: statusFromBackend(t.status),
    time: relativeTime(t.createdAt),
  }
}

const API = "/api/proxy/jobs"

// ------------------------------ Page ----------------------------------------

export default function Page() {
  const [jobs, setJobs] = useState<Job[]>([])
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  // Rolling map of the raw trackers, keyed by id, so we can requery /status
  // just for the ones still in flight without re-fetching /all every tick.
  const trackersRef = useRef<Map<string, JobTracker>>(new Map())

  const applyTrackers = useCallback((next: JobTracker[]) => {
    for (const t of next) trackersRef.current.set(t.id, t)
    const merged = Array.from(trackersRef.current.values())
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      .map(trackerToJob)
    setJobs(merged)
  }, [])

  // Initial load.
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const res = await fetch(`${API}/all`, { cache: "no-store" })
        if (!res.ok) throw new Error(`GET /jobs/all → ${res.status}`)
        const list = (await res.json()) as JobTracker[]
        if (!cancelled) applyTrackers(list)
      } catch (e) {
        console.error("[RTDPS] failed to load jobs:", e)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [applyTrackers])

  // Poll active jobs every 2s so status transitions QUEUED → PROCESSING → COMPLETED
  // show up without a manual refresh. Only polls IDs that are still in flight —
  // O(active) per tick, not O(all).
  useEffect(() => {
    const timer = setInterval(async () => {
      const active = Array.from(trackersRef.current.values()).filter(
        (t) => t.status === "QUEUED" || t.status === "PROCESSING" || t.status === "STARTING" || t.status === "STARTED",
      )
      if (active.length === 0) return
      try {
        const updated = await Promise.all(
          active.map((t) =>
            fetch(`${API}/status/${t.id}`, { cache: "no-store" }).then((r) =>
              r.ok ? (r.json() as Promise<JobTracker>) : null,
            ),
          ),
        )
        const good = updated.filter((t): t is JobTracker => t !== null)
        if (good.length > 0) applyTrackers(good)
      } catch (e) {
        console.error("[RTDPS] status poll failed:", e)
      }
    }, 2000)
    return () => clearInterval(timer)
  }, [applyTrackers])

  const handleUpload = useCallback(
    async (file: File) => {
      setUploading(true)
      setUploadError(null)
      try {
        const form = new FormData()
        form.append("file", file)
        const res = await fetch(`${API}/importCustomers`, { method: "POST", body: form })
        if (!res.ok) {
          const body = await res.text().catch(() => "")
          throw new Error(`Upload failed (${res.status}): ${body}`)
        }
        const body = (await res.json()) as { trackerId: string }
        // Optimistic tracker so the row appears instantly; the poll loop will
        // enrich it with real counters/status on the next tick.
        const optimistic: JobTracker = {
          id: body.trackerId,
          jobType: "IMPORT",
          filePath: null,
          status: "QUEUED",
          executionId: null,
          readCount: 0,
          writeCount: 0,
          skipCount: 0,
          durationMs: 0,
          errorMessage: null,
          createdAt: new Date().toISOString(),
          updatedAt: null,
        }
        applyTrackers([optimistic])
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e)
        console.error("[RTDPS] upload failed:", e)
        setUploadError(msg)
      } finally {
        setUploading(false)
      }
    },
    [applyTrackers],
  )

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b border-border bg-card">
        <div className="mx-auto flex max-w-5xl items-center justify-between gap-4 px-4 py-4 sm:px-6">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary">
              <Database className="h-5 w-5 text-primary-foreground" aria-hidden="true" />
            </div>
            <div>
              <h1 className="text-lg font-semibold leading-tight text-foreground">RTDPS</h1>
              <p className="text-xs text-muted-foreground">Real-Time Data Processing System</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <span className="inline-flex items-center gap-1.5 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-600 dark:text-emerald-400">
              <Activity className="h-3.5 w-3.5" aria-hidden="true" />
              System Online
            </span>
            <ThemeToggle />
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-5xl space-y-6 px-4 py-8 sm:px-6">
        <div>
          <h2 className="text-balance text-2xl font-semibold tracking-tight text-foreground">
            Dashboard
          </h2>
          <p className="mt-1 text-pretty text-sm text-muted-foreground">
            Upload a CSV file to queue a new import job and track progress below.
          </p>
        </div>

        <UploadPanel onUpload={handleUpload} uploading={uploading} error={uploadError} />
        <RecentJobs jobs={jobs} />
      </main>
    </div>
  )
}
