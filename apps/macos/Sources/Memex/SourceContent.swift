import Foundation

/// Typed provider content retained by ingestion. Plain-text lookalikes never
/// become attachments, and an image placeholder is hidden only with its image.
enum SourceContent {
    static func blocks(_ message: Message) -> [RichContentBlock] {
        guard let source = message.sourceContent, let data = source.data(using: .utf8),
              let values = try? JSONSerialization.jsonObject(with: data) as? [[String: Any]] else { return [] }
        return values.compactMap { value in
            let type = value["type"] as? String ?? ""
            let label = value["title"] as? String ?? value["filename"] as? String ?? "Attachment"
            let source = value["source"] as? [String: Any]
            if ["input_image", "image", "image_url", "local_image", "localImage"].contains(type) {
                let mime = source?["media_type"] as? String ?? value["mimeType"] as? String ?? "image/png"
                if let encoded = source?["data"] as? String ?? value["data"] as? String,
                   let image = imageData(encoded) { return .embeddedImage(label: label == "Attachment" ? "Image" : label, data: image, mimeType: mime) }
                let url = value["image_url"] as? String ?? (value["image_url"] as? [String: Any])?["url"] as? String
                    ?? value["url"] as? String ?? source?["url"] as? String ?? value["path"] as? String
                if let url {
                    if url.hasPrefix("data:image/"), let separator = url.range(of: ";base64,"),
                       let image = imageData(String(url[separator.upperBound...])) {
                        return .embeddedImage(label: "Image", data: image, mimeType: String(url.dropFirst(5).prefix(while: { $0 != ";" })))
                    }
                    if ContentLocation.parse(url) != nil { return .attachment(label: label == "Attachment" ? "Image" : label, source: url, image: true) }
                }
            }
            if ["document", "input_file", "file", "attachment"].contains(type) {
                if let path = value["path"] as? String ?? value["file_url"] as? String ?? source?["url"] as? String,
                   ContentLocation.parse(path) != nil { return .attachment(label: label, source: path, image: false) }
                if source?["type"] as? String == "text", let text = source?["data"] as? String {
                    return .code(text, language: "text")
                }
                if source?["type"] as? String == "base64" || value["file_data"] != nil {
                    let mime = source?["media_type"] as? String
                    return .attachmentNotice(label: label,
                        detail: "Embedded document" + (mime.map { " · " + $0 } ?? "") + ". Preview unavailable; contents are retained in the raw transcript.")
                }
                // Unavailable provider file IDs remain inspectable without treating
                // their identifier as a local path or guessing a download URL.
                if let id = value["file_id"] as? String {
                    return .attachmentNotice(label: label, detail: "\(id) · Attachment is stored with the provider.")
                }
                return .attachmentNotice(label: label, detail: "Preview unavailable; attachment details are retained in the raw transcript.")
            }
            return nil
        }
    }

    static func displayText(_ message: Message) -> String {
        // Without typed source content there cannot be an image to replace a
        // placeholder. Avoid scanning large tool transcripts during every page update.
        guard message.sourceContent != nil, message.text.contains("<<ImageDisplayed>>") else { return message.text }
        let imageCount = blocks(message).filter {
            switch $0 { case .embeddedImage, .attachment(_, _, true): true; default: false }
        }.count
        guard imageCount > 0 else { return message.text }
        var remaining = imageCount
        return message.text.components(separatedBy: "\n").filter { line in
            let compact = line.trimmingCharacters(in: .whitespaces)
            guard remaining > 0, compact == "<<ImageDisplayed>>" else { return true }
            remaining -= 1
            return false
        }.joined(separator: "\n")
    }

    private static func imageData(_ encoded: String) -> Data? {
        guard encoded.utf8.count <= 28_000_000, let data = Data(base64Encoded: encoded), data.count <= 20_000_000 else { return nil }
        return data
    }
}
