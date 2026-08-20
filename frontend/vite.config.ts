import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/web/ui/dist",
    emptyOutDir: true,
  },
})
