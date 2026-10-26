import { Ledger } from '../src/ledger.js'
export class MemoryStore {
  value
  failures = 0
  async load() { return structuredClone(this.value) }
  async save(value) { if (this.failures-- > 0) throw new Error('disk unavailable'); this.value = structuredClone(value) }
}
export async function fixture(config = {}) {
  const store = new MemoryStore()
  let now = 1000000000
  const ledger = new Ledger(store, config, () => now)
  await ledger.ready
  return { ledger, store, now: () => now, tick: ms => { now += ms } }
}
export const event = (seq, type, data, time = 1000000000) => ({ seq, type, data, time })
export async function turn(l, sid, seq = 1, time = 1000000000) {
  await l.ingest(sid, event(seq, 'turn/start', { turn: 1 }, time))
  await l.ingest(sid, event(seq+1, 'user/message', { source: { kind: 'user' }, content: [{ type: 'text', text: `request ${sid}` }] }, time))
  await l.ingest(sid, event(seq+2, 'assistant/message', { message: { content: [{ type: 'text', text: `reply ${sid}` }] } }, time))
  await l.ingest(sid, event(seq+3, 'turn/end', { turn: 1, reason: { kind: 'completed' } }, time))
}
/** Controlled 0.1.0 corruption: a late replay of source 3 erased source 6. */
export async function damageReply(ledger,sid='S',start=1) {
 await ledger.ingest(sid,event(start,'turn/start',{turn:1}))
 await ledger.ingest(sid,event(start+1,'user/message',{source:{kind:'user'},content:[{type:'text',text:'request'}]}))
 await ledger.ingest(sid,event(start+2,'assistant/message',{message:{content:[{type:'tool-call',name:'fixture',arguments:'{}'}]}}))
 await ledger.ingest(sid,event(start+3,'assistant/message',{message:{content:[{type:'text',text:'progress'}]}}))
 await ledger.ingest(sid,event(start+5,'assistant/message',{message:{content:[{type:'text',text:'last valid reply'}]}}))
 await ledger.ingest(sid,event(start+6,'turn/end',{turn:1,reason:{kind:'completed'}}))
 await ledger.tx(s=>{
  const row=s.records[`${sid}:${start}`],sourceRef={sessionId:sid,eventSeq:start+2,kind:'assistant/message'}
  ledger.append(s,row.id,`fixture-corruption:${sid}`,'reply_captured',{replyExcerpt:'',replyTruncated:false},ledger.clock(),sourceRef)
 })
}
