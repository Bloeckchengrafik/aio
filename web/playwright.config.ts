import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./tests",
  use: {
    baseURL: "http://127.0.0.1:4173",
    viewport: { width: 1440, height: 1100 },
    reducedMotion: "reduce",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "npm exec vite -- --host 127.0.0.1 --port 4173",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: !process.env.CI,
  },
});
