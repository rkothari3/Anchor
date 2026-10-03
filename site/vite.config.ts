import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// GitHub Pages serves a project site from /<repo>/, so the base comes from the build environment.
export default defineConfig({
  base: process.env.SITE_BASE ?? "/",
  plugins: [react()],
  worker: { format: "es" },
  build: { target: "es2022", sourcemap: false },
});
