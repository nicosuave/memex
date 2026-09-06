import { memo, useMemo, useState } from "react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import type { Message } from "./session"

type XmlField = { label: string; value: string; path: string }
type ToolPayload = Record<string, unknown>
type MarkdownPart =
  | { kind: "text"; content: string }
  | { kind: "section"; tag: string; attributes: string; children: MarkdownPart[] }
type JsonNode =
  | { kind: "object"; entries: { key: string; value: JsonNode }[] }
  | { kind: "array"; values: JsonNode[] }
  | { kind: "primitive"; source: string }

const contextTag = /(?:^|[-_])(?:context|instructions|reminder|environment|permissions|skills|memory|collaboration)(?:$|[-_])/i

// Context envelopes are not XML documents: their bodies can contain Markdown,
// unescaped ampersands, examples, and content whose closing tag has not loaded yet.
function contextParts(content: string): MarkdownPart[] {
  const lines = content.split(/(?<=\n)/)
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
  const escapedAt = (line: string, at: number) => {
    let slashes = 0
    while (at > slashes && line[at - slashes - 1] === "\\") slashes++
    return slashes % 2 === 1
  }
  const hasInlineCloser = (
    lineIndex: number,
    offset: number,
    length: number,
  ) => {
    for (let index = lineIndex; index < lines.length; index++) {
      const candidate = lines[index]
      if (index > lineIndex && !candidate.trim()) return false
      if (index > lineIndex && /^ {0,3}(`{3,}|~{3,})/.test(candidate))
        return false
      if (index > lineIndex && /^ {0,3}#{1,6}(?:[ \t]+|$)/.test(candidate))
        return false
      let at = index === lineIndex ? offset : 0
      while (at < candidate.length) {
        if (candidate[at] !== "`") {
          at++
          continue
        }
        let end = at + 1
        while (candidate[end] === "`") end++
        // Once a code span is open, backslashes have no escaping meaning.
        if (end - at === length) return true
        at = end
      }
    }
    return false
  }
  for (const [lineIndex, line] of lines.entries()) {
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
      const at = ticks.index
      if (inlineTicks) {
        if (inlineTicks === ticks[0].length) inlineTicks = 0
      } else if (
        !escapedAt(line, at) &&
        hasInlineCloser(lineIndex, at + ticks[0].length, ticks[0].length)
      ) {
        inlineTicks = ticks[0].length
      }
    }
  }
  return parts
}

const maxJsonCandidateLength = 256 * 1024
const maxJsonCandidates = 100
const maxJsonDepth = 64

class SourceJsonParser {
  at: number
  constructor(
    private readonly source: string,
    start: number,
    private readonly limit: number,
  ) {
    this.at = start
  }

  private whitespace() {
    while (
      this.at < this.limit &&
      [" ", "\t", "\n", "\r"].includes(this.source[this.at])
    ) this.at++
  }

  private take() {
    if (this.at >= this.limit) return undefined
    return this.source[this.at++]
  }

  private string(): string | null {
    const start = this.at
    if (this.take() !== '"') return null
    while (this.at < this.limit) {
      const character = this.source.charCodeAt(this.at)
      this.at++
      if (character === 0x22) return this.source.slice(start, this.at)
      if (character < 0x20) return null
      if (character !== 0x5c) continue
      const escape = this.take()
      if ('"\\/bfnrt'.includes(escape ?? "")) continue
      if (
        escape !== "u" ||
        this.at + 4 > this.limit ||
        !/^[\da-fA-F]{4}$/.test(this.source.slice(this.at, this.at + 4))
      )
        return null
      this.at += 4
    }
    return null
  }

  value(depth = 0): JsonNode | null {
    if (depth > maxJsonDepth) return null
    this.whitespace()
    const start = this.at
    const character = this.source[this.at]
    if (character === "{") {
      this.at++
      const entries: { key: string; value: JsonNode }[] = []
      this.whitespace()
      if (this.at < this.limit && this.source[this.at] === "}") {
        this.at++
        return { kind: "object", entries }
      }
      while (this.at < this.limit) {
        const key = this.string()
        if (key === null) return null
        this.whitespace()
        if (this.take() !== ":") return null
        const value = this.value(depth + 1)
        if (!value) return null
        entries.push({ key, value })
        this.whitespace()
        const separator = this.take()
        if (separator === "}") return { kind: "object", entries }
        if (separator !== ",") return null
        this.whitespace()
      }
      return null
    }
    if (character === "[") {
      this.at++
      const values: JsonNode[] = []
      this.whitespace()
      if (this.at < this.limit && this.source[this.at] === "]") {
        this.at++
        return { kind: "array", values }
      }
      while (this.at < this.limit) {
        const value = this.value(depth + 1)
        if (!value) return null
        values.push(value)
        this.whitespace()
        const separator = this.take()
        if (separator === "]") return { kind: "array", values }
        if (separator !== ",") return null
      }
      return null
    }
    if (character === '"') {
      const source = this.string()
      return source === null ? null : { kind: "primitive", source }
    }
    const literal = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/
      .exec(this.source.slice(this.at, this.limit))?.[0]
    if (!literal) return null
    this.at += literal.length
    return { kind: "primitive", source: this.source.slice(start, this.at) }
  }
}

function formatJsonNode(node: JsonNode, depth = 0): string {
  const indent = "  ".repeat(depth)
  const childIndent = "  ".repeat(depth + 1)
  if (node.kind === "primitive") return node.source
  if (node.kind === "array") {
    if (!node.values.length) return "[]"
    return `[\n${node.values.map((value) =>
      `${childIndent}${formatJsonNode(value, depth + 1)}`).join(",\n")}\n${indent}]`
  }
  if (!node.entries.length) return "{}"
  return `{\n${node.entries.map(({ key, value }) =>
    `${childIndent}${key}: ${formatJsonNode(value, depth + 1)}`).join(",\n")}\n${indent}}`
}

function formatJsonBlocks(content: string, markdown: boolean) {
  let output = ""
  let offset = 0
  let candidates = 0
  let fence: { character: string; length: number } | null = null
  let changed = false
  while (offset < content.length) {
    const newline = content.indexOf("\n", offset)
    const lineEnd = newline < 0 ? content.length : newline + 1
    const line = content.slice(offset, lineEnd)
    const marker = /^ {0,3}(`{3,}|~{3,})(.*)$/.exec(line.trimEnd())
    if (fence) {
      output += line
      offset = lineEnd
      if (marker && marker[1][0] === fence.character &&
          marker[1].length >= fence.length && !marker[2].trim()) fence = null
      continue
    }
    if (marker && (marker[1][0] !== "`" || !marker[2].includes("`"))) {
      fence = { character: marker[1][0], length: marker[1].length }
      output += line
      offset = lineEnd
      continue
    }
    const beginning = /^ {0,3}(?=[{[])/.exec(line)?.[0]
    if (beginning !== undefined && candidates++ < maxJsonCandidates) {
      const start = offset + beginning.length
      const parser = new SourceJsonParser(
        content,
        start,
        Math.min(content.length, start + maxJsonCandidateLength),
      )
      const parsed = parser.value()
      if (parsed) {
        const valueEnd = parser.at
        const valueLineEnd = content.indexOf("\n", valueEnd)
        const trailingEnd = valueLineEnd < 0 ? content.length : valueLineEnd
        if (/^[ \t\r]*$/.test(content.slice(valueEnd, trailingEnd))) {
          const formatted = formatJsonNode(parsed)
          output += markdown ? `\`\`\`json\n${formatted}\n\`\`\`` : formatted
          output += content.slice(valueEnd, trailingEnd)
          offset = trailingEnd
          changed = true
          continue
        }
      }
    }
    output += line
    offset = lineEnd
  }
  return { content: output, changed }
}

function MarkdownImage({ src, alt, title, literal }: {
  src?: string; alt?: string; title?: string; literal: string
}) {
  const [failedSource, setFailedSource] = useState<string>()
  if (!src || failedSource === src) return <code>{literal}</code>
  return <img src={src} alt={alt ?? ""} title={title} loading="lazy" onError={() => setFailedSource(src)} />
}

function MarkdownContent({ content }: { content: string }) {
  const formatted = useMemo(() => formatJsonBlocks(content, true).content, [content])
  return (
    <div className="markdown">
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={{
        img: ({ src, alt, title, node }) => <MarkdownImage src={typeof src === "string" ? src : undefined} alt={alt} title={title}
          literal={formatted.slice(node?.position?.start.offset, node?.position?.end.offset)} />,
      }}>
        {formatted}
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
  const formatted = formatJsonBlocks(text, false)
  const blockValue =
    typeof value === "object" ||
    text.includes("\n") ||
    formatted.changed ||
    /^(command|code|content|patch|prompt|query|script|sql)$/i.test(name)

  return blockValue ? (
    <pre className="tool-value">{formatted.content}</pre>
  ) : (
    <code className="tool-value-inline">{formatted.content}</code>
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
    return <pre className="tool-content">{formatJsonBlocks(message.content, false).content}</pre>

  if (rendered?.parts) return <ContextContent parts={rendered.parts} />
  if (rendered?.xml) return <XmlMessage parsed={rendered.xml} />
  return <MarkdownContent content={message.content} />
})
