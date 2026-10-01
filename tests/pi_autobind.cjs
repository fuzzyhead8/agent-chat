// Run: node tests/pi_autobind.cjs (uses jiti/typebox from an installed Pi; PI_PACKAGE_DIR overrides discovery).
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const cp = require('node:child_process');
const { createRequire } = require('node:module');

const piDir = process.env.PI_PACKAGE_DIR || path.join(process.env.APPDATA || '/usr/local/lib', 'npm/node_modules/@earendil-works/pi-coding-agent');
const piRequire = createRequire(path.join(piDir, 'package.json'));
const { createJiti } = piRequire('jiti');
const home = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-autobind-'));
const bindingFile = path.join(home, '.pi/agent/agent-chat-wake.json');
fs.mkdirSync(path.dirname(bindingFile), { recursive: true });
const project = { project: 'test-project', root: process.cwd() };
fs.writeFileSync(bindingFile, JSON.stringify({ older: { ...project, session: 'other-agent' } }));
const original = { home: os.homedir, exec: cp.execFile, fetch: global.fetch, interval: global.setInterval, clear: global.clearInterval, env: { ...process.env } };
const calls = [], messages = [], statuses = [], handlers = {}, commands = {}, tools = {};
let offline = false, tick;
os.homedir = () => home;
process.env.AGENT_CHAT_API_TOKEN = 'test-private';
process.env.AGENT_CHAT_SESSION = 'inherited-other-agent';
process.env.AGENT_CHAT_TOKEN = 'inherited-reservation';
delete process.env.AGENT_CHAT_PROJECT;
delete process.env.AGENT_CHAT_ROOT;
cp.execFile = (_file, args, options, done) => {
    calls.push({ args, session: options.env.AGENT_CHAT_SESSION, project: options.env.AGENT_CHAT_PROJECT, reservation: options.env.AGENT_CHAT_TOKEN });
    const response = args[0] === 'register' ? { session: options.env.AGENT_CHAT_SESSION, agent: args[2] } : { messages };
    queueMicrotask(() => done(offline ? new Error('offline') : null, JSON.stringify(response), ''));
};
global.fetch = async () => ({ ok: true, json: async () => ({ enabled: false, blocked: false }) });
global.setInterval = fn => { tick = fn; return 1; };
global.clearInterval = () => { tick = undefined; };
const pi = { on: (name, handler) => { handlers[name] = handler; }, registerTool: tool => { tools[tool.name] = tool; }, registerCommand: (name, command) => { commands[name] = command; }, getThinkingLevel: () => 'xhigh', sendMessage: () => assert.fail('no test wake expected') };
let sessionId = 'new-session-one';
const ctx = { cwd: process.cwd(), hasUI: true, isIdle: () => false, model: { id: 'gpt-6.1-sol' }, sessionManager: { getSessionId: () => sessionId }, ui: { setStatus: (_name, text) => statuses.push(text), notify: () => {} } };
const settle = () => new Promise(resolve => setImmediate(resolve));
const registered = () => calls.filter(c => c.args[0] === 'register');

(async () => {
    try {
        const jiti = createJiti(__filename, { moduleCache: false, fsCache: false, alias: { typebox: piRequire.resolve('typebox') } });
        const extension = await jiti.import(path.join(__dirname, '../integrations/pi/agent-chat-wake.ts'), { default: true });
        extension(pi);
        await handlers.session_start({ reason: 'new' }, ctx); await settle();
        assert.equal(registered().length, 1, 'new Pi session must register automatically');
        const first = JSON.parse(fs.readFileSync(bindingFile, 'utf8'))[sessionId];
        assert.equal(first.project, project.project);
        assert.notEqual(first.session, 'other-agent');
        assert.notEqual(first.session, 'inherited-other-agent');
        assert(statuses.includes('agent-chat: 0 unread'));
        assert(calls.every(c => c.reservation === undefined), 'never inherit a reservation token');
        await handlers.session_start({ reason: 'reload' }, ctx); await settle();
        assert.equal(registered().length, 1, 'reload must reuse the binding');
        assert.equal(JSON.parse(fs.readFileSync(bindingFile, 'utf8'))[sessionId].session, first.session);
        sessionId = 'new-session-two';
        await handlers.session_start({ reason: 'fork' }, ctx); await settle();
        assert.equal(registered().length, 2);
        assert.notEqual(JSON.parse(fs.readFileSync(bindingFile, 'utf8'))[sessionId].session, first.session);
        await commands['agent-chat-unbind'].handler('', ctx);
        await handlers.session_start({ reason: 'reload' }, ctx); await settle();
        assert.equal(registered().length, 2, 'explicit unbind must survive reload');
        sessionId = 'unconfigured'; ctx.cwd = path.join(home, 'unrelated');
        await handlers.session_start({ reason: 'new' }, ctx); await settle();
        assert.equal(registered().length, 2, 'unconfigured projects must stay disconnected');
        sessionId = 'offline-session'; ctx.cwd = process.cwd(); offline = true;
        await handlers.session_start({ reason: 'new' }, ctx); await settle();
        assert(statuses.some(s => s && s.includes('offline')), 'startup failure must be visible, not fatal');
        assert.equal(JSON.parse(fs.readFileSync(bindingFile, 'utf8'))[sessionId], undefined);
        const attemptedSession = registered().at(-1).session;
        offline = false;
        await handlers.session_start({ reason: 'reload' }, ctx); await settle();
        assert.equal(registered().at(-1).session, attemptedSession, 'retry must use the same own identity');
        assert(JSON.parse(fs.readFileSync(bindingFile, 'utf8'))[sessionId]);
        sessionId = 'explicit-project'; ctx.cwd = path.join(home, 'explicit');
        process.env.AGENT_CHAT_PROJECT = 'explicit-project'; process.env.AGENT_CHAT_ROOT = ctx.cwd;
        await handlers.session_start({ reason: 'new' }, ctx); await settle();
        assert.equal(JSON.parse(fs.readFileSync(bindingFile, 'utf8'))[sessionId].project, 'explicit-project');
        console.log('PASS: new/fork auto-register, reload reuse, unique identities, no inherited tokens, persistent unbind, project isolation, offline retry, explicit environment, unread footer');
    } finally {
        if (handlers.session_shutdown) await handlers.session_shutdown();
        os.homedir = original.home; cp.execFile = original.exec; global.fetch = original.fetch; global.setInterval = original.interval; global.clearInterval = original.clear;
        for (const key of Object.keys(process.env)) if (!(key in original.env)) delete process.env[key];
        Object.assign(process.env, original.env);
        fs.rmSync(home, { recursive: true, force: true });
    }
})().catch(error => { console.error(error); process.exitCode = 1; });
