"use client"

import { Clock, Download, Eye, RotateCw, CheckCircle2, Loader2, XCircle, Timer } from "lucide-react"
import { Button } from "@/components/ui/button"

export type JobStatus = "completed" | "processing" | "failed" | "queued"

export type Job = {
  id: string
  type: string
  status: JobStatus
  time: string
}

const statusConfig: Record<
  JobStatus,
  { label: string; icon: typeof CheckCircle2; className: string; spin?: boolean }
> = {
  completed: {
    label: "Completed",
    icon: CheckCircle2,
    className: "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20",
  },
  processing: {
    label: "Processing",
    icon: Loader2,
    className: "bg-primary/10 text-primary border-primary/20",
    spin: true,
  },
  queued: {
    label: "Queued",
    icon: Timer,
    className: "bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20",
  },
  failed: {
    label: "Failed",
    icon: XCircle,
    className: "bg-destructive/10 text-destructive border-destructive/20",
  },
}

function StatusBadge({ status }: { status: JobStatus }) {
  const cfg = statusConfig[status]
  const Icon = cfg.icon
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium ${cfg.className}`}
    >
      <Icon className={`h-3.5 w-3.5 ${cfg.spin ? "animate-spin" : ""}`} aria-hidden="true" />
      {cfg.label}
    </span>
  )
}

type RecentJobsProps = {
  jobs: Job[]
}

export function RecentJobs({ jobs }: RecentJobsProps) {
  return (
    <section
      aria-labelledby="jobs-heading"
      className="rounded-xl border border-border bg-card shadow-sm"
    >
      <div className="flex items-center justify-between gap-2 border-b border-border px-6 py-4">
        <div className="flex items-center gap-2">
          <Clock className="h-5 w-5 text-primary" aria-hidden="true" />
          <h2 id="jobs-heading" className="text-base font-semibold text-card-foreground">
            Recent Jobs
          </h2>
        </div>
        <span className="rounded-full bg-muted px-2.5 py-0.5 text-xs font-medium text-muted-foreground">
          {jobs.length} total
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-border text-xs uppercase tracking-wide text-muted-foreground">
              <th scope="col" className="px-6 py-3 font-medium">
                Job Type
              </th>
              <th scope="col" className="px-6 py-3 font-medium">
                Status
              </th>
              <th scope="col" className="px-6 py-3 font-medium">
                Time
              </th>
              <th scope="col" className="px-6 py-3 text-right font-medium">
                Action
              </th>
            </tr>
          </thead>
          <tbody>
            {jobs.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-6 py-12 text-center text-sm text-muted-foreground">
                  No jobs yet. Upload a file to get started.
                </td>
              </tr>
            ) : (
              jobs.map((job) => (
                <tr
                  key={job.id}
                  className="border-b border-border last:border-0 transition-colors hover:bg-muted/40"
                >
                  <td className="px-6 py-4">
                    <span className="font-medium text-foreground">{job.type}</span>
                    <span className="block font-mono text-xs text-muted-foreground">{job.id}</span>
                  </td>
                  <td className="px-6 py-4">
                    <StatusBadge status={job.status} />
                  </td>
                  <td className="px-6 py-4 text-muted-foreground">{job.time}</td>
                  <td className="px-6 py-4">
                    <div className="flex items-center justify-end gap-1">
                      {job.status === "failed" ? (
                        <Button variant="ghost" size="sm" className="text-muted-foreground">
                          <RotateCw className="h-4 w-4" aria-hidden="true" />
                          Retry
                        </Button>
                      ) : job.status === "completed" ? (
                        <Button variant="ghost" size="sm" className="text-muted-foreground">
                          <Download className="h-4 w-4" aria-hidden="true" />
                          Download
                        </Button>
                      ) : (
                        <Button variant="ghost" size="sm" className="text-muted-foreground">
                          <Eye className="h-4 w-4" aria-hidden="true" />
                          View
                        </Button>
                      )}
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  )
}
