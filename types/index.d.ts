import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-llm'
/** Producer attribution carried by automatic DID messages, not human authorization. */
export interface WhatWeDidSource {
  kind: 'what-we-did'
  deliveryId?: string
  epoch?: number
  from?: number
  through?: number
  createdAt?: number
  recordIds?: string[]
  replace?: boolean
  retired?: boolean
}
declare module '@deepseek-ai/dsh-llm' {
  interface MessageSourceMap { 'what-we-did': WhatWeDidSource }
}
export const name: 'what-we-did'
export const inject: string[]
export const SYSTEM_INSTRUCTIONS: string
export function apply(ctx: Context, config?: Record<string, unknown>): void
