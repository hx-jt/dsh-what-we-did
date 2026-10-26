import test from 'node:test'
import assert from 'node:assert/strict'
import { fixture,event } from './fixtures.mjs'
import { Ledger } from '../src/ledger.js'
const assistant=(seq,content)=>event(seq,'assistant/message',{message:{content}})
const message=(seq,value)=>assistant(seq,[{type:'text',text:value}])
export function replayScenario(mode='tool',body='final text') {
 const middle=mode==='same'?[message(3,'progress'),message(4,'progress'),message(5,'new progress')]:[assistant(3,[{type:'tool-call',name:'fixture',arguments:'{}'}]),message(4,'progress'),assistant(5,[{type:'reasoning',text:'private reasoning'}])]
 return [event(1,'turn/start',{turn:1}),event(2,'user/message',{source:{kind:'user'},content:[{type:'text',text:'request'}]}),...middle,message(6,body),event(7,'turn/end',{turn:1,reason:{kind:'completed'}})]
}
const business=l=>structuredClone({records:l.state.records,events:l.state.events,seq:l.state.seq,completed:l.state.completedHumanTurns})
for(const [name,mode] of [['A tool-only replay','tool'],['B progress/final next turn replay','tool'],['C equal-text replay','same']])test(name,async()=>{
 const {ledger:l,tick}=await fixture(),events=replayScenario(mode)
 for(const e of events)await l.ingest('S',e)
 if(name.startsWith('B '))await l.ingest('S',event(20,'turn/start',{turn:2}))
 const before=business(l);tick(1000)
 for(const e of events)await l.ingest('S',e)
 assert.equal(l.state.records['S:1'].replyExcerpt,'final text')
 assert.deepEqual(business(l),before,'replay must not change any business state')
})
test('D/E empty, mixed and long text preserve excerpt and truncation across replay/reload',async()=>{
 const {ledger:l,store}=await fixture(),events=replayScenario('tool','x'.repeat(1000)).filter(e=>e.type!=='turn/end')
 for(const e of events)await l.ingest('S',e)
 let sourceSeq=8
 for(const content of [[],[{type:'text',text:'   '}],[{type:'reasoning',text:'not a reply'}],[{type:'tool-call',name:'x',arguments:'{}'}]])await l.ingest('S',assistant(sourceSeq++,content))
 assert.equal(l.state.records['S:1'].replyExcerpt,'x'.repeat(360));assert.equal(l.state.records['S:1'].replyTruncated,true)
 const restored=new Ledger(store,{},()=>1000001000,['S']);await restored.ready
 const before=business(restored)
 for(const e of events.filter(Boolean))await restored.ingest('S',e)
 assert.deepEqual(business(restored),before)
 await restored.ingest('S',assistant(50,[{type:'text',text:'mixed text'},{type:'tool-call',arguments:'secret params'},{type:'reasoning',text:'hidden'}]))
 assert.equal(restored.state.records['S:1'].replyExcerpt,'mixed text')
 assert.equal(restored.state.records['S:1'].replyTruncated,false)
})
test('source gaps allow late missing input, older text cannot regress latest; no-op head is durable',async()=>{
 const {ledger:l,store}=await fixture()
 const events=replayScenario('same')
 await l.ingest('S',events[1]) // missing start must remain retryable
 await l.ingest('S',events[0]);await l.ingest('S',message(10,'latest'))
 await l.ingest('S',message(12,'latest')) // no business change, but newer valid source
 const before=business(l)
 await l.ingest('S',message(11,'older text'))
 assert.deepEqual(business(l),before)
 await l.ingest('S',events[1])
 assert.equal(l.state.records['S:1'].requestExcerpt,'request')
 const restored=new Ledger(store,{},()=>1000000010,['S']);await restored.ready
 const stable=business(restored)
 await restored.ingest('S',message(9,'late older text'));await restored.ingest('S',message(12,'latest'))
 assert.deepEqual(business(restored),stable)
})
test('F failed save never acknowledges a source; replay fills missing data after retry',async()=>{
 const {ledger:l,store}=await fixture();await l.ingest('S',event(1,'turn/start',{turn:1}))
 store.failures=3
 await assert.rejects(l.ingest('S',message(8,'must persist')),/disk unavailable/)
 store.failures=0;await l.ingest('S',message(8,'must persist'))
 assert.equal(l.state.records['S:1'].replyExcerpt,'must persist')
 const before=business(l);await l.ingest('S',message(8,'must persist'));assert.deepEqual(business(l),before)
})
test('failed later turn start never acknowledges its messages against an earlier turn',async()=>{
 const {ledger:l,store}=await fixture()
 const first=replayScenario('tool');for(const e of first)await l.ingest('S',e)
 const original=structuredClone(l.state.records['S:1'])
 const second=[event(20,'turn/start',{turn:2}),event(21,'user/message',{source:{kind:'user'},content:[{type:'text',text:'second request'}]}),event(22,'assistant/message',{turn:2,message:{content:[{type:'text',text:'second reply'}]}}),event(23,'turn/end',{turn:2,reason:{kind:'completed'}})]
 store.failures=3;await assert.rejects(l.ingest('S',second[0]),/disk unavailable/)
 store.failures=0
 for(const e of second.slice(1))await l.ingest('S',e)
 for(const e of second)await l.ingest('S',e)
 assert.deepEqual(l.state.records['S:1'],original)
 assert.equal(l.state.records['S:20'].requestExcerpt,'second request')
 assert.equal(l.state.records['S:20'].replyExcerpt,'second reply')
 assert.equal(l.state.records['S:20'].status,'replied')
 assert.equal(l.state.completedHumanTurns,2)
})
test('explicit source turn preserves gaps even when prior end and next start both failed',async()=>{
 const {ledger:l}=await fixture()
 await l.ingest('S',event(1,'turn/start',{turn:1}),1)
 const missingUser=event(21,'user/message',{source:{kind:'user'},content:[{type:'text',text:'second request'}]})
 await l.ingest('S',missingUser,20)
 assert.equal(l.state.records['S:1'].requestExcerpt,'')
 await l.ingest('S',event(20,'turn/start',{turn:2}),20)
 await l.ingest('S',missingUser,20)
 assert.equal(l.state.records['S:20'].requestExcerpt,'second request')
})
test('0.1.0 state without observation receipts upgrades without changing replies or business history',async()=>{
 const {ledger:l,store}=await fixture(),events=replayScenario('same')
 for(const e of events)await l.ingest('S',e)
 const original=business(l)
 delete store.value.ingestion
 const compatible=new Ledger(store,{},()=>1000000020);await compatible.ready
 assert.deepEqual(business(compatible),original)
 for(const e of events)await compatible.ingest('S',e)
 assert.deepEqual(business(compatible),original)
 assert.ok(compatible.state.ingestion.observed.S.includes(4))
})
