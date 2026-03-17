import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type { Connection, NetworkElement } from "@shared/schema";

type SampleCase = {
  elements: NetworkElement[];
  connections: Connection[];
};

const SAMPLE_CACHE = new Map<string, SampleCase>();
const CURRENT_DIR =
  typeof __dirname !== "undefined"
    ? __dirname
    : dirname(fileURLToPath(import.meta.url));

export function loadSampleCase(sampleName: "ieee9" | "ieee30"): SampleCase {
  const cached = SAMPLE_CACHE.get(sampleName);
  if (cached) {
    return structuredClone(cached);
  }

  const filePath = resolve(CURRENT_DIR, "samples", `${sampleName}.json`);
  const parsed = JSON.parse(readFileSync(filePath, "utf-8")) as SampleCase;
  SAMPLE_CACHE.set(sampleName, parsed);
  return structuredClone(parsed);
}
