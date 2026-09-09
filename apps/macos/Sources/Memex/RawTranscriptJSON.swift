import Foundation

/// Retains unknown CLI fields for inspection without coupling the reader to the entire schema.
indirect enum RawTranscriptJSON: Codable {
    case object([String: RawTranscriptJSON])
    case array([RawTranscriptJSON])
    case string(String)
    case number(Decimal)
    case bool(Bool)
    case null

    init(from decoder: Decoder) throws {
        let value = try decoder.singleValueContainer()
        if value.decodeNil() { self = .null }
        else if let boolean = try? value.decode(Bool.self) { self = .bool(boolean) }
        else if let text = try? value.decode(String.self) { self = .string(text) }
        else if let number = try? value.decode(Decimal.self) { self = .number(number) }
        else if let array = try? value.decode([RawTranscriptJSON].self) { self = .array(array) }
        else { self = .object(try value.decode([String: RawTranscriptJSON].self)) }
    }

    func encode(to encoder: Encoder) throws {
        var value = encoder.singleValueContainer()
        switch self {
        case .object(let fields): try value.encode(fields)
        case .array(let elements): try value.encode(elements)
        case .string(let text): try value.encode(text)
        case .number(let number): try value.encode(number)
        case .bool(let boolean): try value.encode(boolean)
        case .null: try value.encodeNil()
        }
    }

    func prettyPrinted() throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        return String(decoding: try encoder.encode(self), as: UTF8.self)
    }
}
