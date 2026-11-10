import { useEffect, useState } from "react"

/** True once `active` has held for `delay` ms, so short waits never flash an indicator. */
export function useDelayedFlag(active: boolean, delay: number) {
  const [elapsed, setElapsed] = useState(false)
  useEffect(() => {
    setElapsed(false)
    if (!active) return
    const timer = setTimeout(() => setElapsed(true), delay)
    return () => clearTimeout(timer)
  }, [active, delay])
  return active && elapsed
}
