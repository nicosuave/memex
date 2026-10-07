import { useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import type { HostPendingRequest } from "@/execution"

type AnswerContext = { id: string; name: string; text: string; byteLength: number }
type AnswerDraft = { selected: string[]; text: string; contexts: AnswerContext[] }
const emptyDraft: AnswerDraft = { selected: [], text: "", contexts: [] }
const maximumBytes = 1_048_576
const maximumFiles = 10

async function captureContext(files: File[], existing: AnswerContext[]): Promise<AnswerContext[]> {
  if (files.length + existing.length > maximumFiles) throw new Error("Attach at most 10 text files to an answer")
  let remaining = maximumBytes - existing.reduce((total, item) => total + item.byteLength, 0)
  const result: AnswerContext[] = []
  for (const file of files) {
    if (/^(image|audio|video)\//.test(file.type) || /application\/(pdf|zip|gzip|x-tar|x-7z-compressed)/i.test(file.type)
      || /\.(pdf|png|jpe?g|gif|webp|heic|mp3|mp4|mov|zip|gz)$/i.test(file.name)) {
      throw new Error("Question replies support UTF-8 text files only. Attach media to a separate prompt.")
    }
    if (file.size > remaining) throw new Error("Question attachments must total 1 MB or less")
    const bytes = new Uint8Array(await file.arrayBuffer())
    if (bytes.includes(0)) throw new Error(`${file.name} is not a text file`)
    let text: string
    try { text = new TextDecoder("utf-8", { fatal: true }).decode(bytes) }
    catch { throw new Error(`${file.name} is not a UTF-8 text file`) }
    remaining -= bytes.length
    result.push({ id: crypto.randomUUID(), name: file.name, text, byteLength: bytes.length })
  }
  return result
}

function encodeAnswer(request: HostPendingRequest, draft: AnswerDraft): string {
  const custom = draft.text.trim()
  const selectedValues = !request.payload.multiSelect && custom ? [] : draft.selected
  const text = [custom, ...draft.contexts.map(item => `Attached answer context: ${item.name}\n${item.text}`)].filter(Boolean).join("\n\n")
  if (!selectedValues.length) return text
  // The native adapters use this tagged envelope so an ordinary JSON answer
  // remains text rather than being misinterpreted as a multi-select response.
  const bytes = new TextEncoder().encode(JSON.stringify({ selectedValues, text }))
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return `sqacp-user-input-v1:${btoa(binary)}`
}

export function ExecutionQuestions({ requests, disabled, blockedRequestIDs, onAnswer }: {
  requests: HostPendingRequest[]
  disabled: boolean
  blockedRequestIDs: Set<string>
  onAnswer: (requestId: string, text: string) => Promise<boolean>
}) {
  const [selectedID, setSelectedID] = useState("")
  const [drafts, setDrafts] = useState<Record<string, AnswerDraft>>({})
  const [submitted, setSubmitted] = useState<Set<string>>(new Set())
  const [capturing, setCapturing] = useState(false)
  const [sending, setSending] = useState(false)
  const [error, setError] = useState("")
  const request = requests.find(item => item.requestId === selectedID) || requests[0]
  const pendingKey = JSON.stringify(requests.map(item => item.requestId))

  useEffect(() => {
    const ids = new Set<string>(JSON.parse(pendingKey))
    setDrafts(current => Object.fromEntries(Object.entries(current).filter(([id]) => ids.has(id))))
    setSubmitted(current => new Set([...current].filter(id => ids.has(id))))
  }, [pendingKey])

  if (!request) return null
  const index = requests.indexOf(request)
  const draft = drafts[request.requestId] || emptyDraft
  const blocked = disabled || capturing || sending || submitted.has(request.requestId) || blockedRequestIDs.has(request.requestId)
  const update = (next: AnswerDraft) => setDrafts(current => ({ ...current, [request.requestId]: next }))
  const toggle = (value: string) => update({ ...draft, selected: request.payload.multiSelect
    ? draft.selected.includes(value) ? draft.selected.filter(item => item !== value) : [...draft.selected, value]
    : draft.selected.includes(value) ? [] : [value] })

  return <section aria-label="Pending questions" className="space-y-2">
    <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
      <Button size="sm" variant="ghost" disabled={index === 0 || capturing || sending} onClick={() => { setSelectedID(requests[index - 1].requestId); setError("") }}>Previous question</Button>
      <span>Question {index + 1} of {requests.length}</span>
      <Button size="sm" variant="ghost" disabled={index + 1 === requests.length || capturing || sending} onClick={() => { setSelectedID(requests[index + 1].requestId); setError("") }}>Next question</Button>
    </div>
    <fieldset className="rounded-lg border p-3" disabled={blocked}>
      <legend className="px-1 text-sm font-medium">{request.payload.title || "Input required"}</legend>
      <p className="mb-2 whitespace-pre-wrap text-sm">{request.payload.prompt}</p>
      {request.payload.choices?.map(choice => <label key={choice.id} className="mb-2 flex items-start gap-2 text-sm">
        <input type={request.payload.multiSelect ? "checkbox" : "radio"} name={request.requestId}
          checked={draft.selected.includes(choice.value)} onChange={() => toggle(choice.value)} />
        <span>{choice.title}{choice.description && <span className="block text-xs text-muted-foreground">{choice.description}</span>}</span>
      </label>)}
      <Input aria-label="Custom answer" type={request.payload.isSecret ? "password" : "text"} value={draft.text}
        onChange={event => update({ ...draft, text: event.target.value })} />
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <label className="rounded border px-2 py-1 text-xs">Attach answer context<input aria-label="Attach answer context" className="sr-only" type="file" multiple
          onChange={async event => {
            const files = Array.from(event.target.files || []); event.target.value = ""
            if (!files.length || blocked) return
            const id = request.requestId
            setCapturing(true); setError("")
            try {
              const contexts = await captureContext(files, draft.contexts)
              setDrafts(current => ({ ...current, [id]: { ...(current[id] || emptyDraft), contexts: [...draft.contexts, ...contexts] } }))
            } catch (error) { setError(String(error)) }
            finally { setCapturing(false) }
          }} /></label>
        {draft.contexts.map(item => <Button key={item.id} size="sm" variant="outline"
          onClick={() => update({ ...draft, contexts: draft.contexts.filter(context => context.id !== item.id) })}>{item.name} ×</Button>)}
        <Button className="ml-auto" size="sm" disabled={!draft.text.trim() && !draft.selected.length && !draft.contexts.length}
          onClick={async () => {
            if (blocked) return
            setSending(true); setError("")
            try {
              if (await onAnswer(request.requestId, encodeAnswer(request, draft))) {
                setSubmitted(current => new Set([...current, request.requestId]))
              }
            } catch (error) { setError(String(error)) }
            finally { setSending(false) }
          }}>Answer</Button>
      </div>
    </fieldset>
    {blockedRequestIDs.has(request.requestId) && <p className="text-xs text-muted-foreground">Answer delivery is unconfirmed. Inspect or retry its exact command below.</p>}
    {submitted.has(request.requestId) && <p className="text-xs text-muted-foreground">Answer sent; waiting for the provider to settle this question.</p>}
    {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
  </section>
}
