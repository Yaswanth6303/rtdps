"use client";

import { useEffect, useState } from "react";
import { FileDown, RefreshCw, AlertCircle, Clock, CheckCircle2 } from "lucide-react";
import { cn } from "@/lib/utils";

type Job = {
  id: string;
  jobType: "IMPORT" | "EXPORT";
  status: "QUEUED" | "PROCESSING" | "COMPLETED" | "FAILED";
  createdAt: string;
  filePath: string | null;
  conflictCount: number;
};

export default function JobDashboard({ refreshTrigger }: { refreshTrigger: number }) {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchJobs = async () => {
    try {
      const res = await fetch("/api/proxy/jobs/all");
      if (res.ok) {
        const data = await res.json();
        setJobs(data);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
    const hasActiveJobs = jobs.some((j) => j.status === "QUEUED" || j.status === "PROCESSING");
    const interval = setInterval(fetchJobs, hasActiveJobs ? 3000 : 10000);
    return () => clearInterval(interval);
  }, [jobs, refreshTrigger]);

  const getStatusIcon = (status: Job["status"]) => {
    switch (status) {
      case "QUEUED":
        return <Clock className="w-4 h-4 text-amber-500" />;
      case "PROCESSING":
        return <RefreshCw className="w-4 h-4 text-blue-500 animate-spin" />;
      case "COMPLETED":
        return <CheckCircle2 className="w-4 h-4 text-green-600" />;
      case "FAILED":
        return <AlertCircle className="w-4 h-4 text-[#CC0000]" />;
    }
  };

  const getStatusBg = (status: Job["status"]) => {
    switch (status) {
      case "QUEUED": return "bg-amber-50 text-amber-700 border-amber-200";
      case "PROCESSING": return "bg-blue-50 text-blue-700 border-blue-200";
      case "COMPLETED": return "bg-green-50 text-green-700 border-green-200";
      case "FAILED": return "bg-red-50 text-[#CC0000] border-red-200";
    }
  };

  if (loading && jobs.length === 0) {
    return (
      <div className="flex justify-center p-12 bg-white border border-zinc-200 rounded">
        <RefreshCw className="w-6 h-6 text-zinc-400 animate-spin" />
      </div>
    );
  }

  return (
    <div className="w-full bg-white border border-zinc-200 rounded overflow-hidden shadow-sm">
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm text-zinc-700">
          <thead className="text-xs uppercase bg-zinc-50 text-zinc-500 border-b border-zinc-200">
            <tr>
              <th className="px-6 py-4 font-semibold tracking-wider">Job Type</th>
              <th className="px-6 py-4 font-semibold tracking-wider">Status</th>
              <th className="px-6 py-4 font-semibold tracking-wider">Time</th>
              <th className="px-6 py-4 font-semibold tracking-wider text-right">Action</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr
                key={job.id}
                className="border-b border-zinc-100 hover:bg-zinc-50 transition-colors last:border-b-0"
              >
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="flex items-center space-x-3">
                    <div className={cn(
                      "w-2 h-2 rounded-full",
                      job.jobType === "IMPORT" ? "bg-[#CC0000]" : "bg-blue-500"
                    )} />
                    <span className="font-semibold text-zinc-900">{job.jobType}</span>
                  </div>
                  <div className="text-xs text-zinc-500 mt-1 font-mono truncate max-w-[150px]">
                    {job.id}
                  </div>
                </td>
                <td className="px-6 py-4">
                  <span className={cn("inline-flex items-center space-x-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border", getStatusBg(job.status))}>
                    {getStatusIcon(job.status)}
                    <span>{job.status}</span>
                  </span>
                  {job.conflictCount > 0 && (
                    <div className="text-xs text-amber-600 mt-1 ml-1 font-medium">
                      {job.conflictCount} conflicts
                    </div>
                  )}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-zinc-500">
                  {new Date(job.createdAt).toLocaleString()}
                </td>
                <td className="px-6 py-4 text-right">
                  {job.jobType === "EXPORT" && job.status === "COMPLETED" ? (
                    <a
                      href={`/api/proxy/jobs/download/${job.id}`}
                      download
                      className="inline-flex items-center space-x-1 text-green-700 hover:text-green-800 transition-colors font-semibold text-sm bg-green-100 hover:bg-green-200 px-3 py-1.5 rounded"
                    >
                      <FileDown className="w-4 h-4" />
                      <span>Download CSV</span>
                    </a>
                  ) : (
                    <span className="text-zinc-300">-</span>
                  )}
                </td>
              </tr>
            ))}
            {jobs.length === 0 && (
              <tr>
                <td colSpan={4} className="px-6 py-12 text-center text-zinc-500">
                  No jobs found. Start by uploading a CSV.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
