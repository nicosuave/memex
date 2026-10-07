import Foundation
import Testing
@testable import Memex

@Suite @MainActor struct AppPreferencesTests {
    @Test func shortcutEditsConflictCheckPersistDisableAndReset() throws {
        let suite = "MemexPreferencesTests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let preferences = AppPreferences(defaults: defaults)
        #expect(preferences.setBinding(.init(key: "f"), for: .newConversation) != nil)
        #expect(preferences.setBinding(.init(key: "q"), for: .newConversation) != nil)
        #expect(preferences.setBinding(.init(key: "p", command: false), for: .newConversation) != nil)
        #expect(preferences.setBinding(.init(key: "p", shift: true), for: .newConversation) == nil)
        #expect(preferences.setBinding(nil, for: .find) == nil)
        let reopened = AppPreferences(defaults: defaults)
        #expect(reopened.binding(for: .newConversation) == .init(key: "p", shift: true))
        #expect(reopened.shortcut(for: .newConversation)?.key.character == "p")
        #expect(reopened.shortcut(for: .find) == nil)
        reopened.resetShortcuts()
        let reset = AppPreferences(defaults: defaults)
        #expect(reset.binding(for: .newConversation) == .init(key: "n"))
        #expect(reset.binding(for: .find) == .init(key: "f"))
    }

    @Test func swappedBindingsSurviveRelaunch() throws {
        let suite = "MemexPreferencesTests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let preferences = AppPreferences(defaults: defaults)
        #expect(preferences.setBinding(nil, for: .refresh) == nil)
        #expect(preferences.setBinding(.init(key: "r"), for: .newConversation) == nil)
        #expect(preferences.setBinding(.init(key: "n"), for: .refresh) == nil)
        let reopened = AppPreferences(defaults: defaults)
        #expect(reopened.binding(for: .newConversation)?.key == "r")
        #expect(reopened.binding(for: .refresh)?.key == "n")
    }

    @Test func appearancePersistsAndChangesRenderedFonts() throws {
        let suite = "MemexPreferencesTests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let preferences = AppPreferences(defaults: defaults)
        preferences.interfaceSize = 19
        preferences.codeSize = 16
        preferences.fontDesign = .monospaced
        preferences.appearance = .dark
        preferences.reduceMotion = true
        preferences.increasedContrast = true
        let reopened = AppPreferences(defaults: defaults)
        #expect(reopened.bodyNSFont.pointSize == 19)
        #expect(reopened.codeNSFont.pointSize == 16)
        #expect(reopened.fontDesign == .monospaced)
        #expect(reopened.appearance == .dark)
        #expect(reopened.reduceMotion && reopened.increasedContrast)
        reopened.resetAppearance()
        #expect(reopened.bodyNSFont.pointSize == 14)
        #expect(reopened.codeNSFont.pointSize == 12)
    }
}
