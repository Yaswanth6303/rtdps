import { NextRequest } from "next/server"

const BACKEND = process.env.RTDPS_API_URL ?? "http://localhost:9191"

// Method-preserving passthrough to the Spring Boot backend. Avoids CORS in
// the browser (frontend only talks to same-origin /api/proxy/*) and centralises
// where the backend URL is configured. Streams the body untouched so multipart
// uploads (POST /jobs/importCustomers) work with no special-casing.
async function forward(req: NextRequest, path: string[]) {
  const target = `${BACKEND}/${path.join("/")}${req.nextUrl.search}`

  // Drop headers Next.js/Node manages itself; keep everything else (Content-Type,
  // Content-Length, etc.) so multipart boundaries survive.
  const headers = new Headers(req.headers)
  headers.delete("host")
  headers.delete("connection")

  const init: RequestInit = {
    method: req.method,
    headers,
    // GET/HEAD must not have a body — Node fetch throws otherwise.
    body: ["GET", "HEAD"].includes(req.method) ? undefined : req.body,
    // Required when streaming a request body in Node.
    // @ts-expect-error - duplex is Node-only, missing from lib.dom
    duplex: "half",
    cache: "no-store",
  }

  const upstream = await fetch(target, init)
  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: upstream.headers,
  })
}

type Ctx = { params: Promise<{ path: string[] }> }

export async function GET(req: NextRequest, ctx: Ctx)    { return forward(req, (await ctx.params).path) }
export async function POST(req: NextRequest, ctx: Ctx)   { return forward(req, (await ctx.params).path) }
export async function PUT(req: NextRequest, ctx: Ctx)    { return forward(req, (await ctx.params).path) }
export async function DELETE(req: NextRequest, ctx: Ctx) { return forward(req, (await ctx.params).path) }
export async function PATCH(req: NextRequest, ctx: Ctx)  { return forward(req, (await ctx.params).path) }
