import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";

export default defineConfig(({ mode }) => ({
  plugins: [wasm(), topLevelAwait(), svelte()],
  base: mode === "production" ? (process.env.BASE_PATH || "/") : "/",
}));
