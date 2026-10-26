import { defineDomain, domainTable } from '@deepseek-ai/dsh-storage-domain'
import z from 'zod'
const json = z.json()
const stateSchema=z.object({format:z.literal(1),seq:z.number().int().nonnegative(),events:z.array(json),records:z.record(z.string(),json),recipients:z.record(z.string(),json),expired:z.record(z.string(),json),archives:z.record(z.string(),json),gaps:z.array(json),maintenance:z.array(json),config:json,configVersion:z.number().int().positive(),configAudit:z.array(json),completedHumanTurns:z.number().int().nonnegative(),lastModelChangeTurn:z.number().nullable(),pendingPolicy:json.nullable(),ingestion:z.object({observed:z.record(z.string(),z.array(z.number().int().nonnegative())),replyHeads:z.record(z.string(),z.number().int().nonnegative())}).default({observed:{},replyHeads:{}})})
export const spec=defineDomain({name:'what_we_did',version:1,tables:{state:domainTable(stateSchema)}})
/** One validated KV value is the atomic persistence unit; no cross-table transaction assumption. */
export function hostStore(ctx) {
  const opened=ctx.storageDomain.open(spec)
  void opened.catch(()=>{})
  return {
    async load() { return (await opened).table('state').get('ledger') },
    async save(value) { await (await opened).table('state').put('ledger',value) },
    async close() { await (await opened).close() },
  }
}
