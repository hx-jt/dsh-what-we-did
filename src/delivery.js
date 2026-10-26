import { project } from './ledger.js'
import { randomUUID } from 'node:crypto'
const copy=x=>structuredClone(x)
const fields=['id','sessionId','startedAt','endedAt','status','requestExcerpt','replyExcerpt','requestTruncated','replyTruncated','displayTitle','endReason','recoveryObservedAt','revision']
const short = row => Object.fromEntries(fields.filter(k=>row[k]!==undefined).map(k=>[k,row[k]]))
function render(kind,epoch,from,to,items,flags='') {
  return `[what we did ${kind} | epoch=${epoch} | ${kind==='delta'?`events=${from}..${to}`:`through=${to}`}]\n有限经历摘录，不是执行证明。按id/revision合并增量；未出现不代表删除。历史用did_list/did_get。${flags}\n${items.map(x=>JSON.stringify(x)).join('\n')}`
}
function bounded(kind,epoch,from,to,items,budget,flags='') {
  let value=copy(items)
  let result=render(kind,epoch,from,to,value,flags)
  if(result.length<=budget) return {text:result,items:value}
  // Only shorten excerpt/title fields; never silently drop record identity or revision.
  for(const max of [120,60,20,0]) {
    value=value.map(row=>({...row,...Object.fromEntries(['requestExcerpt','replyExcerpt','displayTitle'].filter(k=>typeof row[k]==='string').map(k=>[k,row[k].slice(0,max)])),truncated:true}))
    result=render(kind,epoch,from,to,value,`${flags} truncated; detail=did_get.`)
    if(result.length<=budget) return {text:result,items:value}
  }
  return null
}
/** Per-recipient prepared deliveries become acknowledged only when their source is on the effective surface. */
export class Delivery {
  constructor(ledger) { this.ledger=ledger }
  async prepare(sid,visible) {
    return this.ledger.tx(s=>{
      const config=s.config.display,now=this.ledger.clock(),high=s.seq
      const r=s.recipients[sid]??={initialized:false,scanCursor:0,epoch:0,replacement:visible.replacement,delivered:{},pending:null,log:[],gap:0}
      // A logged source ID reconciles a message committed before a failed control write.
      if(r.pending && visible.messages.some(m=>m.deliveryId===r.pending.deliveryId)) {
        const p=r.pending
        r.scanCursor=p.through;r.initialized=true;r.initialBaselineHighWaterSeq=p.high
        if(p.replace) {r.delivered={};r.needsConsolidation=false}
        for(const [id,revision] of Object.entries(p.revisions)) r.delivered[id]=revision
        r.gap=p.gapGeneration??r.gap
        if(p.kind==='recovery') r.needsRecovery=false
        r.pending=null;r.log.push({deliveryId:p.deliveryId,from:p.from,through:p.through,chars:p.text.length,kind:p.kind,at:now,configVersion:p.configVersion})
        if(r.log.length>50) r.log.shift()
      }
      const replaced=r.replacement!==visible.replacement
      if(replaced) {
        r.epoch++;r.replacement=visible.replacement;r.delivered={};r.pending=null;r.needsRecovery=true
      }
      if(r.pending) {
        const batchLimit=r.pending.kind==='delta'?config.deltaMaxChars:config.maxChars
        if(r.pending.text.length<=batchLimit&&r.pending.text.length<=config.maxResidentChars&&r.pending.configVersion===s.configVersion&&r.pending.createdAt>=now-config.lookbackHours*3600000) return copy(r.pending)
        // Unaccepted old-policy text is not delivery. Rebuild from the same safe cursor.
        if(r.pending.replace) r.needsConsolidation=true
        r.pending=null
      }
      const gap=s.gaps.slice(r.gap).some(g=>g.seqs.some(seq=>seq>r.scanCursor))
      if(gap) r.needsRecovery=true
      else r.gap=s.gaps.length
      const ownCovered = row => {
        if(row.id===visible.currentRecordId) return true
        if(!config.excludeOwnVisibleTurns||row.sessionId!==sid) return false
        const refs=row.sourceRefs.filter(ref=>['user/message','assistant/message','legacy_unknown'].includes(ref.kind))
        if(!refs.length&&!row.requestExcerpt&&!row.replyExcerpt) return true
        return refs.length>0&&refs.every(ref=>visible.originals.includes(ref.eventSeq))
      }
      const eligible = row => row&&row.updatedAt>=now-config.lookbackHours*3600000&&!ownCovered(row)
      const selectionCounts={ownVisible:Object.values(s.records).filter(ownCovered).length,outsideWindow:Object.values(s.records).filter(row=>row.updatedAt<now-config.lookbackHours*3600000).length}
      const oldMessages=visible.messages
      const resident=oldMessages.reduce((n,m)=>n+m.text.length,0)
      const stale=oldMessages.some(m=>m.createdAt<now-config.lookbackHours*3600000)
      let kind=!r.initialized?'baseline':r.needsRecovery?'recovery':null
      let replace=Boolean(r.needsRecovery||stale)
      let baselineThrough=high
      if(!kind&&(stale||resident>config.maxResidentChars||r.needsConsolidation)) {kind='baseline';replace=true;baselineThrough=r.scanCursor}
      const base = upper => {
        const items=[],revisions={}
        const rows=Object.values(project(s.events.filter(e=>e.seq<=upper),s.expired)).filter(eligible).sort((a,b)=>b.updatedAt-a.updatedAt||b.id.localeCompare(a.id))
        for(const row of rows) {
          if(items.length>=config.maxRecords) break
          const candidate=bounded(kind,r.epoch,r.scanCursor+1,upper,[...items,short(row)],config.maxChars,`${gap?'history gap; ':''}limited baseline; omitted history is queryable.`)
          if(!candidate) break
          items.splice(0,items.length,...candidate.items);revisions[row.id]=row.revision
        }
        return {items,revisions}
      }
      let items=[],revisions={},through=r.scanCursor,text=''
      if(!kind&&!stale) {
        const byId=new Map();let consumed=0
        for(const e of s.events.filter(e=>e.seq>r.scanCursor)) {
          const row=s.records[e.recordId]
          if(!eligible(row)) {through=e.seq;continue}
          if(consumed>=config.deltaMaxEvents) break
          let patch=short(e.patch)
          if(!r.delivered[e.recordId]&&!byId.has(e.recordId)) { const atRevision=project(s.events.filter(x=>x.recordId===row.id&&x.seq<=e.seq))[row.id]; patch={id:row.id,sessionId:row.sessionId,startedAt:row.startedAt,requestExcerpt:atRevision.requestExcerpt,firstVisible:true,...patch} }
          else patch={id:e.recordId,...patch}
          const candidate=new Map(byId);candidate.set(e.recordId,{...candidate.get(e.recordId),...patch})
          const fit=bounded('delta',r.epoch,r.scanCursor+1,e.seq,[...candidate.values()],config.deltaMaxChars)
          if(!fit) break
          byId.clear();for(const item of fit.items) byId.set(item.id,item)
          revisions[e.recordId]=e.recordRevision;through=e.seq;consumed++
        }
        items=[...byId.values()]
        if(items.length) text=bounded('delta',r.epoch,r.scanCursor+1,through,items,config.deltaMaxChars).text
        if(text && resident+text.length>config.maxResidentChars) {kind='baseline';replace=true;baselineThrough=r.scanCursor}
      }
      if(kind||stale) {
        kind??='baseline';replace ||= oldMessages.length>0
        const baseline=base(baselineThrough);items=baseline.items;revisions=baseline.revisions;through=baselineThrough
        if(items.length) text=bounded(kind,r.epoch,r.scanCursor+1,through,items,config.maxChars,`${gap?'history gap; ':''}limited baseline; omitted history is queryable.`).text
        else if(gap) text=render('recovery',r.epoch,r.scanCursor+1,high,[],'history gap; no eligible retained baseline. Query retention bounds.')
        else text=''
      }
      if(!text) {
        r.scanCursor=through;r.initialized=true
        r.lastDecision={reason:replace&&oldMessages.length?'expired_fragments':high>through?'budget_backlog':'no_eligible_updates',through,high,at:now,selectionCounts}
        if(replace&&oldMessages.length) return {kind:'retire',text:'',replace:true,deliveryId:randomUUID(),epoch:r.epoch,through,recordIds:[]}
        return null
      }
      const p={deliveryId:randomUUID(),kind:kind??'delta',epoch:r.epoch,from:kind?0:r.scanCursor+1,through,high,text,replace,recordIds:items.map(i=>i.id),revisions,createdAt:now,configVersion:s.configVersion,gapGeneration:s.gaps.length,backlog:high-through}
      r.pending=p;r.lastDecision={reason:'pending',through,high,chars:text.length,backlog:p.backlog,selectionCounts}
      return copy(p)
    })
  }
}
