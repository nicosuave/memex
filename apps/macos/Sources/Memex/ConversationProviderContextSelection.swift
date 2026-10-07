import Foundation

#if canImport(SQACPHost)
  import SQACP

  extension ConversationAttachment {
    static func codexSkill(name: String, path: String) throws -> Self {
      guard path.hasPrefix("/"), !path.contains("\0") else {
        throw ConversationRuntimeError(message: "The provider returned an invalid skill path.")
      }
      let uri = URL(fileURLWithPath: path).absoluteString
      let block = AcpPromptContentBlock.resourceLink(
        .init(uri: uri, name: name, nativeInvocation: .codexSkill))
      return Self(
        id: UUID().uuidString, title: name, path: uri, content: try JSONEncoder().encode(block))
    }

    static func codexPlugin(name: String, id: String) throws -> Self {
      guard !id.isEmpty, !id.contains("\0") else {
        throw ConversationRuntimeError(message: "The provider returned an invalid plugin identity.")
      }
      let uri = "plugin://" + id
      let block = AcpPromptContentBlock.resourceLink(
        .init(uri: uri, name: name, nativeInvocation: .codexPlugin))
      return Self(
        id: UUID().uuidString, title: name, path: uri, content: try JSONEncoder().encode(block))
    }
  }
#endif
