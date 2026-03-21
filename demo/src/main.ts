import "./style.css";
import init, { Kinesis } from "./generated/ferrokinesis_wasm.js";

type JsonBody = Record<string, unknown>;

type BannerKind = "info" | "success" | "error";

interface KinesisResponse {
  status: number;
  body: string;
  headers: Record<string, string>;
}

interface DerivedState {
  streamName: string;
  shardId: string | null;
  shardIterator: string | null;
}

interface Preset {
  id: string;
  label: string;
  summary: string;
  target: string;
  buildBody: (state: DerivedState) => JsonBody;
}

const DEMO_STREAM_NAME = "browser-demo-stream";
const DEFAULT_SHARD_ID = "shardId-000000000000";
const DATA_PAYLOAD = "aGVsbG8gZnJvbSBmZXJyb2tpbmVzaXM=";

const KINESIS_OPTIONS = {
  createStreamMs: 0,
  deleteStreamMs: 0,
  updateStreamMs: 0,
};

const PRESETS: Preset[] = [
  {
    id: "create-stream",
    label: "CreateStream",
    summary: "Create a one-shard stream so the rest of the demo has something to work with.",
    target: "Kinesis_20131202.CreateStream",
    buildBody: (state) => ({
      StreamName: state.streamName,
      ShardCount: 1,
    }),
  },
  {
    id: "put-record",
    label: "PutRecord",
    summary: "Write one base64 payload into the demo stream with a stable partition key.",
    target: "Kinesis_20131202.PutRecord",
    buildBody: (state) => ({
      StreamName: state.streamName,
      PartitionKey: "browser-demo-key",
      Data: DATA_PAYLOAD,
    }),
  },
  {
    id: "get-shard-iterator",
    label: "GetShardIterator",
    summary: "Get a TRIM_HORIZON iterator for the first shard so records can be read back.",
    target: "Kinesis_20131202.GetShardIterator",
    buildBody: (state) => ({
      StreamName: state.streamName,
      ShardId: state.shardId ?? DEFAULT_SHARD_ID,
      ShardIteratorType: "TRIM_HORIZON",
    }),
  },
  {
    id: "get-records",
    label: "GetRecords",
    summary: "Read records using the last iterator returned from the previous response.",
    target: "Kinesis_20131202.GetRecords",
    buildBody: (state) => ({
      ShardIterator:
        state.shardIterator ?? "run GetShardIterator first to populate this field",
    }),
  },
  {
    id: "list-streams",
    label: "ListStreams",
    summary: "List the streams currently alive inside this in-browser emulator instance.",
    target: "Kinesis_20131202.ListStreams",
    buildBody: () => ({}),
  },
];

const presetList = mustById<HTMLDivElement>("preset-list");
const presetSummary = mustById<HTMLParagraphElement>("preset-summary");
const targetInput = mustById<HTMLInputElement>("target-input");
const bodyInput = mustById<HTMLTextAreaElement>("body-input");
const sendRequestButton = mustById<HTMLButtonElement>("send-request");
const resetStateButton = mustById<HTMLButtonElement>("reset-state");
const banner = mustById<HTMLParagraphElement>("banner");
const responseStatus = mustById<HTMLElement>("response-status");
const responseDuration = mustById<HTMLElement>("response-duration");
const headersOutput = mustById<HTMLElement>("headers-output");
const responseOutput = mustById<HTMLElement>("response-output");

let wasmInitPromise: Promise<unknown> | null = null;
let kinesis: Kinesis | null = null;
let derivedState = initialDerivedState();

void boot();

async function boot(): Promise<void> {
  renderPresetButtons();
  clearResponsePanel();
  setPending(true);
  setBanner("info", "Starting a fresh in-browser Kinesis instance...");

  try {
    kinesis = await createKinesis();
    selectPreset(PRESETS[0].id);
    setBanner(
      "success",
      "Fresh in-memory instance ready. State resets on refresh or when you click Reset state.",
    );
  } catch (error) {
    renderClientError(error);
    setBanner("error", errorMessage(error));
  } finally {
    setPending(false);
  }
}

function renderPresetButtons(): void {
  presetList.replaceChildren(
    ...PRESETS.map((preset) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "preset-button";
      button.dataset.presetId = preset.id;
      button.setAttribute("data-testid", `preset-${preset.id}`);
      appendOperationLabel(button, preset.label);
      button.addEventListener("click", () => selectPreset(preset.id));
      return button;
    }),
  );
}

function selectPreset(presetId: string): void {
  const preset = mustPreset(presetId);
  presetSummary.textContent = preset.summary;
  targetInput.value = preset.target;
  bodyInput.value = formatJson(preset.buildBody(derivedState));

  for (const button of presetList.querySelectorAll<HTMLButtonElement>(".preset-button")) {
    button.dataset.active = String(button.dataset.presetId === presetId);
  }
}

sendRequestButton.addEventListener("click", () => {
  void sendRequest();
});

resetStateButton.addEventListener("click", () => {
  void resetState();
});

async function sendRequest(): Promise<void> {
  if (!kinesis) {
    setBanner("error", "The browser demo is still booting. Try again in a moment.");
    return;
  }

  const target = targetInput.value.trim();
  if (!target) {
    setBanner("error", "Target is required.");
    return;
  }

  const bodyText = bodyInput.value.trim();
  const parsed = parseJson(bodyText);
  if (!parsed.ok) {
    setBanner("error", parsed.message);
    return;
  }

  setPending(true);
  setBanner("info", `Sending ${target}...`);

  const startedAt = performance.now();

  try {
    syncDerivedStateFromRequest(parsed.value);
    const response = (await kinesis.request(target, bodyText)) as KinesisResponse;
    syncDerivedStateFromResponse(target, response.body);
    renderResponse(response, performance.now() - startedAt);
    setBanner(
      response.status >= 400 ? "error" : "success",
      `${target} completed with HTTP ${response.status}.`,
    );
  } catch (error) {
    renderClientError(error);
    setBanner("error", errorMessage(error));
  } finally {
    setPending(false);
  }
}

async function resetState(): Promise<void> {
  setPending(true);
  setBanner("info", "Resetting browser state...");

  try {
    derivedState = initialDerivedState();
    kinesis = await createKinesis();
    clearResponsePanel();
    selectPreset(PRESETS[0].id);
    setBanner(
      "success",
      "State reset. You now have a fresh in-browser Kinesis instance with no streams.",
    );
  } catch (error) {
    renderClientError(error);
    setBanner("error", errorMessage(error));
  } finally {
    setPending(false);
  }
}

async function createKinesis(): Promise<Kinesis> {
  await ensureWasmInitialized();
  return new Kinesis(KINESIS_OPTIONS);
}

async function ensureWasmInitialized(): Promise<void> {
  wasmInitPromise ??= init();
  await wasmInitPromise;
}

function clearResponsePanel(): void {
  responseStatus.textContent = "Ready";
  responseStatus.dataset.statusClass = "pending";
  responseDuration.textContent = "--";
  headersOutput.textContent = "{}";
  responseOutput.textContent =
    "Select a preset and send a request to see the response payload.";
}

function renderResponse(response: KinesisResponse, durationMs: number): void {
  responseStatus.textContent = String(response.status);
  responseStatus.dataset.statusClass =
    response.status >= 500 ? "server-error" : response.status >= 400 ? "client-error" : "ok";
  responseDuration.textContent = `${durationMs.toFixed(1)} ms`;
  headersOutput.textContent = formatJson(response.headers);
  responseOutput.textContent = formatMaybeJsonString(response.body);
}

function renderClientError(error: unknown): void {
  responseStatus.textContent = "Client error";
  responseStatus.dataset.statusClass = "client-error";
  responseDuration.textContent = "--";
  headersOutput.textContent = "{}";
  responseOutput.textContent = errorMessage(error);
}

function setPending(isPending: boolean): void {
  sendRequestButton.disabled = isPending;
  resetStateButton.disabled = isPending;
  targetInput.disabled = isPending;
  bodyInput.disabled = isPending;
  sendRequestButton.textContent = isPending ? "Working..." : "Send request";
}

function setBanner(kind: BannerKind, message: string): void {
  banner.textContent = message;
  banner.dataset.kind = kind;
}

function syncDerivedStateFromRequest(body: JsonBody): void {
  if (typeof body.StreamName === "string" && body.StreamName.length > 0) {
    derivedState.streamName = body.StreamName;
  }
}

function syncDerivedStateFromResponse(target: string, responseBody: string): void {
  const decoded = parseJson(responseBody);
  if (!decoded.ok) {
    return;
  }

  if (target.endsWith(".PutRecord") && typeof decoded.value.ShardId === "string") {
    derivedState.shardId = decoded.value.ShardId;
  }

  if (
    target.endsWith(".GetShardIterator") &&
    typeof decoded.value.ShardIterator === "string"
  ) {
    derivedState.shardIterator = decoded.value.ShardIterator;
  }
}

function parseJson(input: string): { ok: true; value: JsonBody } | { ok: false; message: string } {
  try {
    const value = JSON.parse(input) as unknown;
    if (!isRecord(value)) {
      return { ok: false, message: "JSON body must be an object." };
    }
    return { ok: true, value };
  } catch (error) {
    return {
      ok: false,
      message: `Invalid JSON body: ${errorMessage(error)}`,
    };
  }
}

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function formatMaybeJsonString(input: string): string {
  if (input.length === 0) {
    return "{}";
  }

  try {
    return JSON.stringify(JSON.parse(input), null, 2);
  } catch {
    return input;
  }
}

function initialDerivedState(): DerivedState {
  return {
    streamName: DEMO_STREAM_NAME,
    shardId: null,
    shardIterator: null,
  };
}

function mustById<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) {
    throw new Error(`Missing required element: #${id}`);
  }
  return element as T;
}

function mustPreset(id: string): Preset {
  const preset = PRESETS.find((candidate) => candidate.id === id);
  if (!preset) {
    throw new Error(`Unknown preset: ${id}`);
  }
  return preset;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function isRecord(value: unknown): value is JsonBody {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function appendOperationLabel(button: HTMLButtonElement, label: string): void {
  const parts = label.split(/(?=[A-Z])/).filter((part) => part.length > 0);

  parts.forEach((part, index) => {
    button.append(part);
    if (index < parts.length - 1) {
      button.append(document.createElement("wbr"));
    }
  });
}
