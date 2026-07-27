"use client";

import { useState, useRef } from "react";
import { UploadCloud, CheckCircle, AlertCircle, Loader2 } from "lucide-react";
import { cn } from "@/lib/utils";

export default function UploadZone({ onUploadSuccess }: { onUploadSuccess: () => void }) {
  const [isDragging, setIsDragging] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [policy, setPolicy] = useState<"SKIP" | "UPDATE">("SKIP");
  const [status, setStatus] = useState<"idle" | "uploading" | "success" | "error">("idle");
  const [message, setMessage] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") setIsDragging(true);
    else if (e.type === "dragleave") setIsDragging(false);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const droppedFile = e.dataTransfer.files[0];
      if (droppedFile.name.endsWith(".csv")) {
        setFile(droppedFile);
        setStatus("idle");
      } else {
        setStatus("error");
        setMessage("Only CSV files are allowed.");
      }
    }
  };

  const handleUpload = async () => {
    if (!file) return;
    setStatus("uploading");
    
    const formData = new FormData();
    formData.append("file", file);
    formData.append("conflictPolicy", policy);

    try {
      const res = await fetch("/api/proxy/jobs/importCustomers", {
        method: "POST",
        body: formData,
      });
      const data = await res.json();

      if (res.ok) {
        setStatus("success");
        setMessage(`Job queued! Tracker ID: ${data.trackerId}`);
        setFile(null);
        onUploadSuccess();
        setTimeout(() => setStatus("idle"), 4000);
      } else {
        setStatus("error");
        setMessage(data.error || data.message || "Upload failed");
      }
    } catch (err) {
      setStatus("error");
      setMessage("Network error occurred");
    }
  };

  return (
    <div className="w-full max-w-2xl mx-auto space-y-4">
      <div
        className={cn(
          "relative group rounded border-2 border-dashed p-10 transition-colors duration-200 cursor-pointer bg-zinc-50",
          isDragging ? "border-[#CC0000] bg-red-50" : "border-zinc-300 hover:border-[#CC0000] hover:bg-red-50/50"
        )}
        onDragEnter={handleDrag}
        onDragLeave={handleDrag}
        onDragOver={handleDrag}
        onDrop={handleDrop}
        onClick={() => inputRef.current?.click()}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".csv"
          className="hidden"
          onChange={(e) => {
            if (e.target.files?.[0]) {
              setFile(e.target.files[0]);
              setStatus("idle");
            }
          }}
        />
        <div className="flex flex-col items-center justify-center text-center space-y-4">
          <div className="p-3 rounded-full bg-white border border-zinc-200 text-zinc-500 group-hover:text-[#CC0000] group-hover:border-red-200 transition-colors shadow-sm">
            <UploadCloud className="w-8 h-8" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-zinc-900">
              {file ? file.name : "Upload CSV Data"}
            </h3>
            <p className="text-sm text-zinc-500 mt-1">
              Drag and drop your file here, or click to browse
            </p>
          </div>
        </div>
      </div>

      {file && status !== "success" && (
        <div className="flex items-center justify-between p-4 bg-zinc-50 border border-zinc-200 rounded">
          <div className="flex items-center space-x-3">
            <span className="text-sm text-zinc-600 font-medium">Conflict Policy:</span>
            <select
              value={policy}
              onChange={(e) => setPolicy(e.target.value as "SKIP" | "UPDATE")}
              className="bg-white border border-zinc-300 text-zinc-900 text-sm rounded focus:ring-[#CC0000] focus:border-[#CC0000] block p-2 outline-none shadow-sm cursor-pointer"
            >
              <option value="SKIP">SKIP (Ignore existing)</option>
              <option value="UPDATE">UPDATE (Overwrite existing)</option>
            </select>
          </div>
          <button
            onClick={handleUpload}
            disabled={status === "uploading"}
            className="flex items-center space-x-2 bg-[#CC0000] hover:bg-[#aa0000] text-white px-5 py-2 rounded transition-colors disabled:opacity-50 font-medium shadow-sm"
          >
            {status === "uploading" ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                <span>Uploading...</span>
              </>
            ) : (
              <span>Process File</span>
            )}
          </button>
        </div>
      )}

      {status === "success" && (
        <div className="flex items-center space-x-2 text-green-700 p-4 bg-green-50 rounded border border-green-200">
          <CheckCircle className="w-5 h-5" />
          <span className="text-sm font-medium">{message}</span>
        </div>
      )}

      {status === "error" && (
        <div className="flex items-center space-x-2 text-[#CC0000] p-4 bg-red-50 rounded border border-red-200">
          <AlertCircle className="w-5 h-5" />
          <span className="text-sm font-medium">{message}</span>
        </div>
      )}
    </div>
  );
}
