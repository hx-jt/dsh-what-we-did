import { evidencePath } from './evidence.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { writeFileSync } from 'node:fs'
import { boot, send, Llm, texts } from './host.mjs'
test('real loop: independent admission, deliveryId reconciliation and owned surface replacement', async () => {
  const { ctx, adapter, agent: create } = await boot()
  try {
    let committed = 0
    ctx.on('session/event', (_s, e) => { if (e.type === 'user/message' && e.data.source.kind === 'what-we-did') committed++ })
    ctx.systemPrompt.context({ name: 'task-ui:snapshot', order: 1, text: 'TASK_FIXTURE_UNCHANGED' })
    let delivery = true
    ctx.on('agent/pre-step', async (_payload, next) => {
      const decision = await next()
      if (decision.kind !== 'enter' || !delivery) return decision
      delivery = false
      return { ...decision, messages: [...decision.messages, Llm.createUserMessage({ content: [{ type: 'text', text: 'INDEPENDENT_DELTA' }], source: { kind: 'what-we-did', deliveryId: 'capability-1' } })] }
    })
    const a = await create('capability-A')
    assert.equal(adapter.requests.length, 0)
    await send(a, 'human original')
    assert.equal(committed, 1)
    assert.match(texts(adapter.requests[0]), /INDEPENDENT_DELTA/)
    const events = a.session.snapshotEvents()
    const owned = events.find(e => e.type === 'user/message' && e.data.source.deliveryId === 'capability-1')
    assert.ok(a.session.surface.nodes.includes(owned.seq))
    // A fresh process can inspect this persisted log; only the stable ID is needed.
    const restored = ctx.sessions.create('capability-restored', { seed: events })
    assert.ok(restored.snapshotEvents().some(e => e.type === 'user/message' && e.data.source.deliveryId === 'capability-1'))
    const before = [...a.session.surface.nodes]
    const replacement = a.session.append('user/message', Llm.createUserMessage({ content: [{ type: 'text', text: 'BOUNDED_BASELINE' }], source: { kind: 'what-we-did', deliveryId: 'capability-2' } }), { surfaceOp: { op: 'replace', startSeq: owned.seq, endSeq: owned.seq }, sourceEventSeqs: [owned.seq] })
    assert.equal(a.session.surface.replaceGeneration, 1)
    assert.deepEqual(a.session.surface.nodes, before.map(seq => seq === owned.seq ? replacement.seq : seq))
    await send(a)
    assert.doesNotMatch(texts(adapter.requests[1]), /INDEPENDENT_DELTA/)
    assert.match(texts(adapter.requests[1]), /BOUNDED_BASELINE/)
    assert.equal(adapter.requests[1].messages.filter(m => m.content.some(b => b.type === 'text' && b.text.includes('TASK_FIXTURE_UNCHANGED'))).length, 1)
    a.session.append('developer/message', { turn: 2, step: 1, message: Llm.createMessage({ role: 'developer', content: [], source: { kind: 'what-we-did', retired: true } }) }, { surfaceOp: { op: 'replace', startSeq: replacement.seq, endSeq: replacement.seq }, sourceEventSeqs: [replacement.seq] })
    await send(a)
    assert.doesNotMatch(texts(adapter.requests[2]), /BOUNDED_BASELINE|INDEPENDENT_DELTA/)
    writeFileSync(evidencePath('host-capabilities.json'), JSON.stringify({ committed, replacementGeneration: a.session.surface.replaceGeneration, requests: adapter.requests.map(r => r.messages) }, null, 2))
  } finally { await ctx.fiber.dispose() }
})
