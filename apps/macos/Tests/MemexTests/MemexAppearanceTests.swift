import AppKit
import SwiftUI
import Testing
@testable import Memex

@Suite @MainActor struct MemexAppearanceTests {
    @Test func highContrastTracksSystemThemeAndAppChoice() {
        #expect(MemexAppearancePolicy.appearanceName(mode: .system, systemDark: false,
            systemContrast: false, increasedContrast: true) == .accessibilityHighContrastAqua)
        #expect(MemexAppearancePolicy.appearanceName(mode: .system, systemDark: true,
            systemContrast: false, increasedContrast: true) == .accessibilityHighContrastDarkAqua)
        #expect(MemexAppearancePolicy.appearanceName(mode: .light, systemDark: true,
            systemContrast: false, increasedContrast: true) == .accessibilityHighContrastAqua)
        #expect(MemexAppearancePolicy.appearanceName(mode: .dark, systemDark: false,
            systemContrast: false, increasedContrast: true) == .accessibilityHighContrastDarkAqua)
    }

    @Test func disablingAppOverridePreservesSystemContrastOrRestoresInheritedAppearance() {
        #expect(MemexAppearancePolicy.appearanceName(mode: .system, systemDark: false,
            systemContrast: true, increasedContrast: false) == .accessibilityHighContrastAqua)
        #expect(MemexAppearancePolicy.appearanceName(mode: .system, systemDark: true,
            systemContrast: true, increasedContrast: false) == .accessibilityHighContrastDarkAqua)
        #expect(MemexAppearancePolicy.appearanceName(mode: .system, systemDark: true,
            systemContrast: false, increasedContrast: false) == nil)
        #expect(MemexAppearancePolicy.appearanceName(mode: .dark, systemDark: false,
            systemContrast: false, increasedContrast: false) == .darkAqua)
    }

    @Test func appMotionOverrideUsesWritableAppOwnedEnvironment() {
        var environment = EnvironmentValues()
        #expect(!environment.memexReduceMotion)
        environment.memexReduceMotion = true
        #expect(environment.memexReduceMotion)
        environment.memexReduceMotion = false
        #expect(!environment.memexReduceMotion)
    }
}
