import { NextRequest } from "next/server";

export const maxDuration = 300; // Allow long-running exports/imports up to 5 minutes
export const dynamic = "force-dynamic";

const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:9191";
const API_USER = process.env.API_USER || "admin";
const API_PASSWORD = process.env.API_PASSWORD || "admin";

const getBasicAuthHeader = () => {
  return "Basic " + Buffer.from(`${API_USER}:${API_PASSWORD}`).toString("base64");
};

async function handleRequest(request: NextRequest, { params }: { params: Promise<{ path: string[] }> }) {
  const resolvedParams = await params;
  const path = resolvedParams.path.join("/");
  const url = new URL(request.url);
  const searchParams = url.searchParams.toString();
  
  const targetUrl = `${BACKEND_URL}/${path}${searchParams ? `?${searchParams}` : ""}`;

  const headers = new Headers(request.headers);
  headers.set("Authorization", getBasicAuthHeader());
  headers.delete("host"); // Let fetch set the host
  headers.delete("connection");

  try {
    const backendResponse = await fetch(targetUrl, {
      method: request.method,
      headers,
      body: request.method !== "GET" && request.method !== "HEAD" ? await request.blob() : undefined,
      duplex: "half",
    } as RequestInit);

    const responseHeaders = new Headers(backendResponse.headers);
    // Don't forward chunked encoding header, let NextJS handle encoding
    responseHeaders.delete("transfer-encoding");
    responseHeaders.delete("content-encoding");

    return new Response(backendResponse.body, {
      status: backendResponse.status,
      statusText: backendResponse.statusText,
      headers: responseHeaders,
    });
  } catch (error) {
    console.error("Proxy error:", error);
    return new Response(JSON.stringify({ error: "Failed to connect to backend" }), {
      status: 502,
      headers: { "Content-Type": "application/json" },
    });
  }
}

export const GET = handleRequest;
export const POST = handleRequest;
export const PUT = handleRequest;
export const DELETE = handleRequest;
export const PATCH = handleRequest;
