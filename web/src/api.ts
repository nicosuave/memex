export const authenticationRequiredMessage = "Authentication required"

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message)
  }
}

async function exchangeBootstrapToken() {
  const fragment = new URLSearchParams(window.location.hash.slice(1))
  const bootstrap = fragment.get("bootstrap")
  if (!bootstrap) return

  history.replaceState(
    null,
    "",
    `${window.location.pathname}${window.location.search}`,
  )
  const response = await fetch("/auth/exchange", {
    method: "POST",
    headers: { Authorization: `Bearer ${bootstrap}` },
    credentials: "same-origin",
  })
  if (!response.ok) {
    const existingSession = await fetch("/api/stats", {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    })
    if (existingSession.status !== 401) return
    throw new ApiError(authenticationRequiredMessage, 401)
  }
}

const authenticationReady = exchangeBootstrapToken()

export async function api<T>(path: string, signal?: AbortSignal): Promise<T> {
  try {
    await authenticationReady
  } catch (error) {
    if (error instanceof ApiError) throw error
    throw new ApiError(authenticationRequiredMessage, 401)
  }
  signal?.throwIfAborted()
  const response = await fetch(path, {
    signal,
    headers: { Accept: "application/json" },
    credentials: "same-origin",
  })
  const data = (await response
    .json()
    .catch(() => ({ error: `HTTP ${response.status}` }))) as T & {
    error?: string
  }
  if (!response.ok) {
    if (response.status === 401) {
      throw new ApiError(authenticationRequiredMessage, response.status)
    }
    throw new ApiError(data.error || `HTTP ${response.status}`, response.status)
  }
  return data
}
