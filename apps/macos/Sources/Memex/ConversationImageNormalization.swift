import Foundation
import ImageIO
import UniformTypeIdentifiers

/// Decode with a bounded thumbnail rather than allocating full camera-image pixels.
/// Accepted small images retain their original bytes; conversion is captured once.
enum ConversationImageNormalization {
    struct Image: Equatable { let data: Data; let mimeType: String }
    static let maximumInputBytes = 20 * 1024 * 1024
    static let maximumOutputBytes = 3 * 1024 * 1024

    static func normalize(_ data: Data) throws -> Image {
        guard data.count <= maximumInputBytes,
              let source = CGImageSourceCreateWithData(data as CFData, [kCGImageSourceShouldCache: false] as CFDictionary),
              let identifier = CGImageSourceGetType(source),
              let type = UTType(identifier as String) else {
            throw ConversationRuntimeError(message: "The image is invalid or exceeds 20 MB.")
        }
        if let mime = type.preferredMIMEType,
           ["image/png", "image/jpeg", "image/gif", "image/webp"].contains(mime),
           data.count <= maximumOutputBytes {
            return Image(data: data, mimeType: mime)
        }
        guard CGImageSourceGetCount(source) == 1 else {
            throw ConversationRuntimeError(message: "This animated or multi-image file is too large or unsupported. Export one image before attaching.")
        }
        for dimension in [2048, 1536, 1024, 768] {
            let options: [CFString: Any] = [kCGImageSourceCreateThumbnailFromImageAlways: true,
                kCGImageSourceCreateThumbnailWithTransform: true, kCGImageSourceThumbnailMaxPixelSize: dimension,
                kCGImageSourceShouldCacheImmediately: true]
            guard let image = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary) else { break }
            // PNG preserves transparency and avoids hiding context against black.
            let output = NSMutableData()
            guard let destination = CGImageDestinationCreateWithData(output, UTType.png.identifier as CFString, 1, nil) else { break }
            CGImageDestinationAddImage(destination, image, nil)
            if CGImageDestinationFinalize(destination), output.length <= maximumOutputBytes {
                return Image(data: output as Data, mimeType: "image/png")
            }
        }
        throw ConversationRuntimeError(message: "The image could not be normalized below 3 MB. Export a smaller image and attach it again.")
    }
}
