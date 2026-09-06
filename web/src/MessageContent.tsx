import { memo, useMemo, useState } from "react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import type { Message } from "./session"

type XmlField = { label: string; value: string; path: string }
type ToolPayload = Record<string, unknown>
type MarkdownPart =
  | { kind: "text"; content: string }
  | { kind: "section"; tag: string; attributes: string; children: MarkdownPart[] }

const contextTag = /(?:^|[-_])(?:context|instructions|reminder|environment|permissions|skills|memory|collaboration)(?:$|[-_])/i

// Context envelopes are not XML documents: their bodies can contain Markdown,
// unescaped ampersands, examples, and content whose closing tag has not loaded yet.
function contextParts(content: string): MarkdownPart[] {
  const parts: MarkdownPart[] = []
  const stack: Extract<MarkdownPart, { kind: "section" }>[] = []
  let fence: { character: string; length: number } | null = null
  let inlineTicks = 0
  const append = (text: string) => {
    const children = stack.at(-1)?.children ?? parts
    const previous = children.at(-1)
    if (previous?.kind === "text") previous.content += text
    else children.push({ kind: "text", content: text })
  }
  for (const line of content.split(/(?<=\n)/)) {
    const marker = /^ {0,3}(`{3,}|~{3,})(.*)$/.exec(line.trimEnd())
    if (fence) {
      append(line)
      if (
        marker && marker[1][0] === fence.character &&
        marker[1].length >= fence.length && !marker[2].trim()
      ) fence = null
      continue
    }
    if (!inlineTicks && marker &&
        (marker[1][0] !== "`" || !marker[2].includes("`"))) {
      fence = { character: marker[1][0], length: marker[1].length }
      append(line)
      continue
    }
    const tag = !inlineTicks && /^ {0,3}<(\/?)([\w.-]+)([^>\n]*)>[ \t]*(?:\r?\n)?$/.exec(line)
    if (tag && contextTag.test(tag[2]) && !tag[3].trimEnd().endsWith("/")) {
      if (!tag[1]) {
        const section: Extract<MarkdownPart, { kind: "section" }> = {
          kind: "section", tag: tag[2], attributes: tag[3].trim(), children: [],
        }
        const children = stack.at(-1)?.children ?? parts
        children.push(section)
        stack.push(section)
        continue
      }
      if (stack.at(-1)?.tag === tag[2] && !tag[3].trim()) {
        stack.pop()
        continue
      }
    }
    append(line)
    // Multiline code spans may contain something that looks like a wrapper.
    for (const ticks of line.matchAll(/`+/g)) {
      if (!inlineTicks) inlineTicks = ticks[0].length
      else if (inlineTicks === ticks[0].length) inlineTicks = 0
    }
  }
  return parts
}

function MarkdownImage({ src, alt, title, literal }: {
  src?: string; alt?: string; title?: string; literal: string
}) {
  const [failedSource, setFailedSource] = useState<string>()
  if (!src || failedSource === src) return <code>{literal}</code>
  return <img src={src} alt={alt ?? ""} title={title} loading="lazy" onError={() => setFailedSource(src)} />
}

function MarkdownContent({ content }: { content: string }) {
  return (
    <div className="markdown">
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={{
        img: ({ src, alt, title, node }) => <MarkdownImage src={typeof src === "string" ? src : undefined} alt={alt} title={title}
          literal={content.slice(node?.position?.start.offset, node?.position?.end.offset)} />,
      }}>
        {content}
      </ReactMarkdown>
    </div>
  )
}

function ContextContent({ parts }: { parts: MarkdownPart[] }) {
  return parts.map((part, index) => part.kind === "text"
    ? <MarkdownContent key={index} content={part.content} />
    : <section className="context-section" key={index}>
        <div className="context-title">{formatToolLabel(part.tag)}
          {part.attributes && <code> {part.attributes}</code>}
        </div>
        <ContextContent parts={part.children} />
      </section>)
}

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
  // Flattening mixed XML content would silently discard text around children.
  if (Array.from(documentNode.querySelectorAll("*")).some((node) =>
    node.children.length && Array.from(node.childNodes).some((child) =>
      (child.nodeType === Node.TEXT_NODE || child.nodeType === Node.CDATA_SECTION_NODE) &&
      child.textContent?.trim()))) return null
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
  const rendered = useMemo(
    () => {
      if (["tool_use", "tool_result"].includes(message.role)) return null
      const parts = contextParts(message.content)
      if (parts.some((part) => part.kind === "section")) return { parts }
      return { xml: parseXml(message.content) }
    },
    [message.content, message.role],
  )
  if (message.role === "tool_use")
    return <ToolCallContent content={message.content} />

  if (message.role === "tool_result")
    return <pre className="tool-content">{message.content}</pre>

  if (rendered?.parts) return <ContextContent parts={rendered.parts} />
  if (rendered?.xml) return <XmlMessage parsed={rendered.xml} />
  return <MarkdownContent content={message.content} />
})
