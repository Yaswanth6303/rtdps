"use client";

import { useState } from "react";
import UploadZone from "@/components/UploadZone";
import JobDashboard from "@/components/JobDashboard";
import ExportButton from "@/components/ExportButton";
import { Database, Activity } from "lucide-react";

export default function Home() {
  const [refreshTrigger, setRefreshTrigger] = useState(0);

  const handleJobQueued = () => {
    setRefreshTrigger((prev) => prev + 1);
  };

  return (
    <main className="min-h-screen bg-zinc-50 text-zinc-900 font-sans selection:bg-[#CC0000]/20 pb-12">
      {/* Classic Navigation Bar */}
      <nav className="bg-[#CC0000] text-white shadow-md">
        <div className="max-w-5xl mx-auto px-6 h-16 flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <Database className="w-5 h-5" />
            <span className="text-lg font-bold tracking-tight">RTDPS Admin</span>
          </div>
          <div className="flex items-center space-x-2 text-sm font-medium bg-red-800/50 px-3 py-1 rounded">
            <Activity className="w-4 h-4 text-green-400" />
            <span>System Online</span>
          </div>
        </div>
      </nav>

      <div className="max-w-5xl mx-auto px-6 mt-10 space-y-10">
        
        {/* Header Section */}
        <div>
          <h1 className="text-3xl font-bold text-zinc-900 tracking-tight">
            Customer Data Processing
          </h1>
          <p className="text-zinc-600 mt-2 text-lg">
            Import new customer records or manage active processing tasks.
          </p>
        </div>

        {/* Centered Upload Section */}
        <section className="bg-white border border-zinc-200 rounded p-8 shadow-sm">
          <h2 className="text-xl font-semibold mb-6 text-zinc-800 border-b border-zinc-100 pb-3">
            Import Customers
          </h2>
          <div className="max-w-3xl mx-auto py-4">
            <UploadZone onUploadSuccess={handleJobQueued} />
          </div>
        </section>

        {/* Jobs & Export Section */}
        <section className="space-y-4">
          <div className="flex items-center justify-between border-b border-zinc-200 pb-3">
            <h2 className="text-xl font-semibold text-zinc-800">Recent Jobs</h2>
            <div className="w-48">
              <ExportButton onExportQueued={handleJobQueued} />
            </div>
          </div>
          <div className="bg-white rounded shadow-sm">
            <JobDashboard refreshTrigger={refreshTrigger} />
          </div>
        </section>

      </div>
    </main>
  );
}
