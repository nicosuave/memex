import { memo, useMemo } from "react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import type { Message } from "./session"

type XmlField = { label: string; value: string; path: string }
type ToolPayload = Record<string, unknown>

const rustDebugString = /\bString\(("(?:\\.|[^"\\])*")\)/g
const rustDebugStaticBoolean = /\bStatic\(Bool\((true|false)\)\)/g
const rustDebugStaticNull = /\bStatic\(Null\)/g
const rustDebugStaticNumber =
  /\bStatic\((?:I64|U64|F64)\((-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)\)\)/g
const rustDebugBoolean = /\bBool\((true|false)\)/g
const rustDebugNumber =
  /\bNumber\((-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)\)/g

function parseToolPayload(content: string): ToolPayload | null {
  const source = content.trim()
  if (!source.startsWith("{") || !source.endsWith("}")) return null

  for (const normalize of [false, true]) {
    const candidate = normalize
      ? source
          .replace(rustDebugString, "$1")
          .replace(rustDebugStaticBoolean, "$1")
          .replace(rustDebugStaticNull, "null")
          .replace(rustDebugStaticNumber, "$1")
          .replace(rustDebugBoolean, "$1")
          .replace(rustDebugNumber, "$1")
      : source
    try {
      const value = JSON.parse(candidate) as unknown
      if (value && typeof value === "object" && !Array.isArray(value)) {
        return value as ToolPayload
      }
    } catch {
      // Try the normalized representation before falling back to raw text.
    }
  }

  return null
}

function formatToolLabel(value: string) {
  return value.replace(/[-_]+/g, " ")
}

function ToolValue({ name, value }: { name: string; value: unknown }) {
  const text =
    typeof value === "string"
      ? value
      : typeof value === "undefined"
        ? "undefined"
        : (JSON.stringify(value, null, 2) ?? String(value))
  const blockValue =
    typeof value === "object" ||
    text.includes("\n") ||
    /^(command|code|content|patch|prompt|query|script|sql)$/i.test(name)

  return blockValue ? (
    <pre className="tool-value">{text}</pre>
  ) : (
    <code className="tool-value-inline">{text}</code>
  )
}

function ToolCallContent({ content }: { content: string }) {
  const payload = useMemo(() => parseToolPayload(content), [content])
  if (!payload) return <pre className="tool-content">{content}</pre>

  const description =
    typeof payload.description === "string" ? payload.description : null
  const fields = Object.entries(payload).filter(
    ([name]) => name !== "description",
  )

  return (
    <div className="tool-call">
      {description && <p className="tool-call-description">{description}</p>}
      {fields.length > 0 && (
        <dl className="tool-fields">
          {fields.map(([name, value]) => (
            <div className="tool-field" key={name}>
              <dt>{formatToolLabel(name)}</dt>
              <dd>
                <ToolValue name={name} value={value} />
              </dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  )
}

function parseXml(
  content: string,
): { title: string; fields: XmlField[] } | null {
  const source = content.trim()
  if (!/^<[A-Za-z_][\w:.-]*(?:\s[^>]*)?>[\s\S]*>$/.test(source)) return null

  const parser = new DOMParser()
  let documentNode = parser.parseFromString(source, "application/xml")
  let root = documentNode.documentElement
  let fragment = documentNode.querySelector("parsererror") !== null
  if (fragment) {
    documentNode = parser.parseFromString(
      `<memex-fragment>${source}</memex-fragment>`,
      "application/xml",
    )
    if (documentNode.querySelector("parsererror")) return null
    root = documentNode.documentElement
    if (!root.children.length) return null
  }

  const fields: XmlField[] = []
  const walk = (node: Element, parentPath = "") => {
    const path = parentPath ? `${parentPath}/${node.tagName}` : node.tagName
    if (!node.children.length) {
      fields.push({
        label: node.tagName.replace(/[-_]+/g, " "),
        value: node.textContent?.trim() || "",
        path,
      })
      return
    }
    Array.from(node.children).forEach((child) => walk(child, path))
  }

  if (fragment) Array.from(root.children).forEach((child) => walk(child))
  else walk(root)

  return {
    title: fragment
      ? "structured message"
      : root.tagName.replace(/[-_]+/g, " "),
    fields,
  }
}

function XmlMessage({
  parsed,
}: {
  parsed: NonNullable<ReturnType<typeof parseXml>>
}) {
  return (
    <div className="xml-card">
      <div className="xml-title">{parsed.title}</div>
      <dl>
        {parsed.fields.map((field, index) => (
          <div
            className="xml-row"
            key={`${field.path}-${index}`}
            title={field.path}
          >
            <dt>{field.label}</dt>
            <dd>{field.value}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}

export const MessageContent = memo(function MessageContent({
  message,
}: {
  message: Message
}) {
  const parsed = useMemo(
    () =>
      ["tool_use", "tool_result"].includes(message.role)
        ? null
        : parseXml(message.content),
    [message.content, message.role],
  )
  if (message.role === "tool_use")
    return <ToolCallContent content={message.content} />

  if (message.role === "tool_result")
    return <pre className="tool-content">{message.content}</pre>

  if (parsed) return <XmlMessage parsed={parsed} />

  return (
    <div className="markdown">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>
        {message.content}
      </ReactMarkdown>
    </div>
  )
})
