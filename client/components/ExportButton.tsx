"use client";

import { useState } from "react";
import { DownloadCloud, Loader2 } from "lucide-react";

export default function ExportButton({ onExportQueued }: { onExportQueued: () => void }) {
  const [isExporting, setIsExporting] = useState(false);

  const handleExport = async () => {
    setIsExporting(true);
    try {
      const res = await fetch("/api/proxy/jobs/exportCustomers", { method: "POST" });
      if (res.ok) {
        onExportQueued();
      }
    } catch (error) {
      console.error(error);
    } finally {
      setTimeout(() => setIsExporting(false), 1000);
    }
  };

  return (
    <button
      onClick={handleExport}
      disabled={isExporting}
      className="flex w-full items-center justify-center space-x-2 bg-white hover:bg-zinc-50 border border-zinc-200 text-[#CC0000] px-6 py-3 rounded transition-colors disabled:opacity-50 font-semibold shadow-sm"
    >
      {isExporting ? (
        <Loader2 className="w-5 h-5 animate-spin" />
      ) : (
        <DownloadCloud className="w-5 h-5" />
      )}
      <span>Export Database</span>
    </button>
  );
}
