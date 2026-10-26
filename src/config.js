/** Global defaults; characters are deterministic UTF-16 lengths, not tokens. */
export const defaults = {
  retention: { mode: 'forever' },
  display: { lookbackHours: 24, maxRecords: 12, maxChars: 4000, maxResidentChars: 8000, deltaMaxEvents: 20, deltaMaxChars: 2000, excludeOwnVisibleTurns: true },
  modelAdjustment: {
    enabled: true, allowedFields: ['display.lookbackHours','display.maxRecords','display.maxChars','display.maxResidentChars'],
    bounds: { 'display.lookbackHours': {min:1,max:168}, 'display.maxRecords': {min:1,max:50}, 'display.maxChars': {min:500,max:12000}, 'display.maxResidentChars': {min:2000,max:20000} },
    userLockedFields: [], minTurnsBetweenChanges: 10,
  },
}
export function configFor(input = {}) {
  const c = structuredClone(defaults)
  for (const key of Object.keys(input)) {
    if (!Object.hasOwn(c,key)) throw new Error(`unknown config field ${key}`)
    c[key] = key === 'retention' ? structuredClone(input[key]) : {...c[key], ...input[key]}
  }
  const d=c.display, r=c.retention, m=c.modelAdjustment
  for (const key of Object.keys(d)) {
    if (!Object.hasOwn(defaults.display,key)) throw new Error(`unknown display field ${key}`)
    if (key==='excludeOwnVisibleTurns') { if (typeof d[key]!=='boolean') throw new Error('excludeOwnVisibleTurns must be boolean'); continue }
    if (!Number.isFinite(d[key]) || d[key]<=0 || (key!=='lookbackHours' && !Number.isSafeInteger(d[key]))) throw new Error(`invalid display.${key}`)
  }
  if (Math.min(d.maxChars,d.deltaMaxChars)<500) throw new Error('batch bound minimum 500 characters')
  if (d.maxChars>d.maxResidentChars || d.deltaMaxChars>d.maxResidentChars) throw new Error('batch exceeds resident bound')
  if (!['forever','ttl'].includes(r.mode)) throw new Error('invalid retention mode')
  if (r.mode==='ttl') {
    if (!['archive','delete'].includes(r.expiryAction)) throw new Error('ttl requires explicit expiryAction')
    if (!Number.isSafeInteger(r.durationMs)||r.durationMs<=0) throw new Error('invalid retention duration')
    if (d.lookbackHours*3600000>r.durationMs) throw new Error('display exceeds retention')
  }
  if (typeof m.enabled!=='boolean' || !Array.isArray(m.allowedFields) || !Array.isArray(m.userLockedFields) || !Number.isSafeInteger(m.minTurnsBetweenChanges) || m.minTurnsBetweenChanges<0) throw new Error('invalid modelAdjustment')
  for (const [field,b] of Object.entries(m.bounds)) if (!Number.isFinite(b.min)||!Number.isFinite(b.max)||b.min>b.max) throw new Error(`invalid bound ${field}`)
  return c
}
/** Only leaf display settings and retention policy are mutable by tools. */
export function patched(config, patch) {
  const next=structuredClone(config)
  for (const [key,value] of Object.entries(patch)) {
    if (key==='retention') next.retention=structuredClone(value)
    else if (key.startsWith('display.') && Object.hasOwn(defaults.display,key.slice(8))) next.display[key.slice(8)]=value
    else throw new Error(`unsupported config field ${key}`)
  }
  return configFor(next)
}
