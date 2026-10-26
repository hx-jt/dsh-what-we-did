import { createRequire } from 'node:module'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { resolve } from 'node:path'
export const root = fileURLToPath(new URL('../../deepseek-harness/', import.meta.url))
const requireHost = createRequire(resolve(root, 'packages/core/agent-loop/package.json'))
export const dep = name => import(pathToFileURL(requireHost.resolve(name)))
export const host = path => import(pathToFileURL(resolve(root, `packages/${path}/lib/index.js`)))
export const { Context } = await dep('@deepseek-ai/cordis')
export const Llm = await dep('@deepseek-ai/dsh-llm')
export const Session = await dep('@deepseek-ai/dsh-session')
export class Capture extends Llm.LlmAdapter {
  requests = []
  nextTool = null
  script = []
  toolCount = 0
  fail = false
  async resolveModel(provider, model) { return { provider, id: model, name: model } }
  async *stream(options) {
    this.requests.push(options)
    if (this.fail) throw new Error('capture injected request failure')
    const scripted=this.script.shift()
    if(this.nextTool||scripted?.tool) {
      const name=this.nextTool??scripted.tool;this.nextTool=null
      const id=Llm.ToolCallId(this.toolCount++===0?'capture-tool-call':`capture-tool-call-${this.toolCount}`)
      if(scripted?.text) {
        yield {type:'block-start',index:1,blockType:'text'}
        yield {type:'text-delta',index:1,text:scripted.text}
        yield {type:'block-end',index:1,block:{type:'text',text:scripted.text}}
      }
      yield {type:'block-start',index:0,blockType:'tool-call'}
      yield {type:'tool-call-delta',index:0,id,name,argumentsDelta:'{}'}
      yield {type:'block-end',index:0,block:{type:'tool-call',id,name,arguments:'{}'}}
      yield {type:'usage',usage:{inputTokens:1,outputTokens:1}}
      yield {type:'finish',reason:{kind:'tool-calls'}}
      return
    }
    const response=scripted?.text??'captured response'
    yield { type: 'block-start', index: 0, blockType: 'text' }
    yield { type: 'text-delta', index: 0, text: response }
    yield { type: 'block-end', index: 0, block: { type: 'text', text: response } }
    yield { type: 'usage', usage: { inputTokens: 1, outputTokens: 1 } }
    yield { type: 'finish', reason: { kind: 'stop' } }
  }
}
export async function boot() {
  const ctx = new Context()
  for (const name of ['@deepseek-ai/dsh-llm', '@deepseek-ai/dsh-session', '@deepseek-ai/dsh-session-projection', '@deepseek-ai/dsh-system-prompt', '@deepseek-ai/dsh-tools', '@deepseek-ai/dsh-agent', '@deepseek-ai/dsh-agent-loop']) {
    await ctx.plugin((await dep(name)).default, name.endsWith('agent-loop') ? { agents: [] } : {})
  }
  const adapter = new Capture()
  ctx.llm.registerAdapter(['capture'], adapter)
  return { ctx, adapter, async agent(id) { return ctx.agentLoop.create(Session.SessionId(id), { provider: 'capture', model: 'capture' }) } }
}
export async function send(agent, text = '继续') {
  agent.followup(Llm.createUserMessage({ content: [{ type: 'text', text }], source: { kind: 'user' } }))
  await agent.whenIdle()
}
export const texts = request => JSON.stringify(request.messages)
