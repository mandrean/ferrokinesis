import { defineConfig } from "vite";

const base = process.env.VITE_BASE_PATH ?? "/";

export default defineConfig({
  base,
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
