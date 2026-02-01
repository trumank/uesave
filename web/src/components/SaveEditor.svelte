<script lang="ts">
  import { JSONEditor, type Content } from "svelte-jsoneditor";
  import { savToJson, jsonToSav } from "../lib/wasm";
  import { readFileAsArrayBuffer, downloadFile } from "../lib/fileHandling";
  import licenses from "../lib/licenses.json";

  let filename = $state("");
  let content: Content = $state({ json: undefined });
  let error = $state("");
  let loading = $state(false);
  let hasContent = $state(false);
  let hasChanges = $state(false);
  let showAbout = $state(false);
  let editorRef: { get: () => Content } | undefined = $state();

  async function handleFile(file: File) {
    error = "";
    loading = true;
    filename = file.name;
    hasContent = false;

    try {
      const data = await readFileAsArrayBuffer(file);
      const json = await savToJson(data);
      content = { text: json };
      hasContent = true;
      hasChanges = false;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      content = { json: undefined };
    } finally {
      loading = false;
    }
  }

  function handleFileInput(e: Event) {
    const input = e.target as HTMLInputElement;
    const file = input.files?.[0];
    if (file) {
      handleFile(file);
    }
  }

  function handleDrop(e: DragEvent) {
    e.preventDefault();
    const file = e.dataTransfer?.files[0];
    if (file) {
      handleFile(file);
    }
  }

  function handleDragOver(e: DragEvent) {
    e.preventDefault();
  }

  function handleContentChange(updatedContent: Content) {
    content = updatedContent;
    hasChanges = true;
  }

  async function handleDownload() {
    error = "";
    loading = true;

    try {
      const currentContent = editorRef?.get() ?? content;
      const jsonStr =
        "text" in currentContent && currentContent.text !== undefined
          ? currentContent.text
          : JSON.stringify(currentContent.json);

      const data = await jsonToSav(jsonStr);
      const outFilename = filename || "save.sav";
      downloadFile(data, outFilename);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }

</script>

<div class="app" ondrop={handleDrop} ondragover={handleDragOver} role="application">
  <header>
    <h1>
      <svg width="22" height="22" viewBox="0 0 32 32">
        <rect x="4" y="2" width="24" height="28" rx="2" fill="currentColor"/>
        <rect x="8" y="2" width="12" height="10" fill="#fff"/>
        <rect x="8" y="18" width="16" height="8" rx="1" fill="rgba(255,255,255,0.3)"/>
        <rect x="14" y="4" width="4" height="6" fill="currentColor"/>
      </svg>
      uesave editor
    </h1>
    {#if hasContent}
      <span class="filename">{filename}</span>
      <button onclick={handleDownload} disabled={loading || !hasChanges}>
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
          <polyline points="7 10 12 15 17 10"/>
          <line x1="12" y1="15" x2="12" y2="3"/>
        </svg>
        save
      </button>
    {/if}
    <div class="spacer"></div>
    <button class="icon-btn" onclick={() => showAbout = true} title="About">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="12" r="10"/>
        <line x1="12" y1="16" x2="12" y2="12"/>
        <line x1="12" y1="8" x2="12.01" y2="8"/>
      </svg>
    </button>
    <a href="https://github.com/trumank/uesave" target="_blank" rel="noopener" class="icon-btn" title="View on GitHub">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
        <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
      </svg>
    </a>
  </header>

  {#if showAbout}
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="modal-backdrop" onclick={() => showAbout = false} onkeydown={(e) => e.key === 'Escape' && (showAbout = false)}>
      <div class="modal" onclick={(e) => e.stopPropagation()} onkeydown={(e) => e.stopPropagation()} role="dialog" tabindex="-1">
        <h2>uesave editor</h2>
        <p>A browser-based editor for Unreal Engine save files (.sav).</p>
        <p>Built with <a href="https://github.com/trumank/uesave" target="_blank" rel="noopener">uesave</a> compiled to WebAssembly.</p>

        <h3>Rust Dependencies</h3>
        {#each licenses.rust as license}
          <h4>{license.name}</h4>
          <p class="packages-list">
            {#each license.packages as pkg, i}
              <a href={pkg.repository || `https://crates.io/crates/${pkg.name}`} target="_blank" rel="noopener">{pkg.name} {pkg.version}</a>{#if i < license.packages.length - 1}, {/if}
            {/each}
          </p>
          {#if license.text}
            <pre>{license.text}</pre>
          {/if}
        {/each}

        <h3>JavaScript Dependencies</h3>
        {#each licenses.js as license}
          <h4>{license.name}</h4>
          <p class="packages-list">
            {#each license.packages as pkg, i}
              <a href={pkg.repository || `https://www.npmjs.com/package/${pkg.name}`} target="_blank" rel="noopener">{pkg.name} {pkg.version}</a>{#if i < license.packages.length - 1}, {/if}
            {/each}
          </p>
          {#if license.text}
            <pre>{license.text}</pre>
          {/if}
        {/each}

        <button class="close-btn" onclick={() => showAbout = false}>close</button>
      </div>
    </div>
  {/if}

  {#if error}
    <div class="error">{error}</div>
  {/if}

  <main>
    {#if hasContent}
      <div class="editor">
        <JSONEditor
          bind:this={editorRef}
          {content}
          onChange={handleContentChange}
          mode="tree"
        />
      </div>
    {:else}
      <div class="welcome">
        <label class="drop-zone" class:loading>
          <input type="file" accept=".sav" onchange={handleFileInput} />
          <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
            <polyline points="17 8 12 3 7 8"/>
            <line x1="12" y1="3" x2="12" y2="15"/>
          </svg>
          {#if loading}
            <span>Loading...</span>
          {:else}
            <span>Drop .sav file here or click to browse</span>
          {/if}
        </label>
      </div>
    {/if}
  </main>
</div>

<style>
  .app {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: #f8f9fa;

    --jse-theme-color: #4d9a4d;
    --jse-theme-color-highlight: #3d8a3d;
    --jse-button-primary-background: #4d9a4d;
    --jse-button-primary-background-highlight: #3d8a3d;
    --jse-button-secondary-background: #e8f5e9;
    --jse-button-secondary-background-highlight: #c8e6c9;
    --jse-button-secondary-color: #2e7d32;
    --jse-selection-background-color: #c8e6c9;
    --jse-selection-background-inactive-color: #e8f5e9;
    --jse-context-menu-button-background-highlight: #e8f5e9;
    --jse-collapsed-items-link-color: #4d9a4d;
    --jse-panel-background: #f1f8f1;
    --jse-panel-border: #c8e6c9;
  }

  header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.75rem 1rem;
    background: white;
    border-bottom: 1px solid #e0e0e0;
  }

  h1 {
    margin: 0;
    font-size: 1.1rem;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    color: #4d9a4d;
  }

  .filename {
    color: #666;
    font-size: 0.875rem;
    margin-left: 1rem;
    margin-right: 0.75rem;
    padding-left: 1rem;
    border-left: 1px solid #e0e0e0;
  }

  .spacer {
    flex: 1;
  }

  header button:not(.icon-btn) {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.3rem 0.65rem;
    background: #4d9a4d;
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.8rem;
    transition: background 0.15s;
  }

  header button:not(.icon-btn):hover:not(:disabled) {
    background: #3d8a3d;
  }

  header button:not(.icon-btn):disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  header .icon-btn {
    color: #666;
    background: none;
    border: none;
    padding: 0.25rem;
    display: flex;
    align-items: center;
    cursor: pointer;
  }

  header .icon-btn:hover {
    color: #1e1e1e;
    background: none;
  }

  .modal-backdrop {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.5);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
  }

  .modal {
    background: white;
    border-radius: 8px;
    padding: 1.5rem;
    width: 90%;
    max-width: 800px;
    max-height: 80vh;
    overflow-y: auto;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
  }

  .modal h2 {
    margin: 0 0 1rem;
    color: #4d9a4d;
  }

  .modal h3 {
    margin: 1.5rem 0 0.5rem;
    font-size: 1rem;
    color: #333;
    border-bottom: 1px solid #e0e0e0;
    padding-bottom: 0.25rem;
  }

  .modal h4 {
    margin: 1rem 0 0.25rem;
    font-size: 0.875rem;
    color: #666;
  }

  .packages-list {
    margin: 0 0 0.5rem;
    font-size: 0.8rem;
    line-height: 1.6;
  }

  .modal p {
    margin: 0.5rem 0;
    line-height: 1.5;
  }

  .modal a {
    color: #4d9a4d;
  }

  .modal pre {
    background: #f5f5f5;
    padding: 1rem;
    border-radius: 4px;
    font-size: 0.75rem;
    overflow-x: auto;
    white-space: pre-wrap;
    word-break: break-word;
  }

  .close-btn {
    margin-top: 1rem;
    padding: 0.4rem 1rem;
    background: #e0e0e0;
    color: #333;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.875rem;
  }

  .close-btn:hover {
    background: #d0d0d0;
  }

  .error {
    padding: 0.75rem 1.5rem;
    background: #ffebee;
    color: #c62828;
    border-bottom: 1px solid #ffcdd2;
  }

  main {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  .welcome {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 2rem;
  }

  .drop-zone {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 1rem;
    width: 100%;
    max-width: 400px;
    aspect-ratio: 4/3;
    border: 2px dashed #ccc;
    border-radius: 12px;
    background: white;
    color: #666;
    cursor: pointer;
    transition: all 0.15s;
  }

  .drop-zone:hover {
    border-color: #4d9a4d;
    color: #4d9a4d;
  }

  .drop-zone.loading {
    pointer-events: none;
    opacity: 0.7;
  }

  .drop-zone input {
    display: none;
  }

  .drop-zone span {
    font-size: 0.95rem;
  }

  .editor {
    flex: 1;
    min-height: 0;
    overflow: hidden;
  }

</style>
