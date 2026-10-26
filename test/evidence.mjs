import { mkdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
/** This repair's evidence never overwrites the 0.1.0 acceptance batch. */
export function evidencePath(name) {
 const directory=process.env.WWD_EVIDENCE_DIR??fileURLToPath(new URL('../.artifacts/evidence/',import.meta.url))
 mkdirSync(directory,{recursive:true})
 return resolve(directory,name)
}
