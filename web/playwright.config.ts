import { defineConfig, devices } from "@playwright/test"
import { tmpdir } from "node:os"
import path from "node:path"

export default defineConfig({
  testDir: "./tests",
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  outputDir: path.join(tmpdir(), "memex-playwright", String(process.pid)),
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? "github" : "line",
  use: {
    ...devices["Desktop Chrome"],
    baseURL: "http://127.0.0.1:4173",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "bun run dev --port 4173 --strictPort",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: !process.env.CI,
  },
})
