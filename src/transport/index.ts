/**
 * Transport factory — selects and configures the MCP transport based on TRANSPORT env var.
 *
 * Supported transports:
 *   stdio (default) — Standard I/O (child process pipes)
 *   sse             — Server-Sent Events over HTTP
 *   http            — Streamable HTTP (MCP 2025-03-26 spec)
 */
import type { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { logger } from '../utils/logging.js';
import { VERSION, SERVER_NAME } from '../utils/version.js';
import { createHttpServer, type NowAIKitHttpServer } from './http-server.js';
import { isDelegatedAuthEnabled, parseDelegatedAuthHeaders, runWithDelegatedAuth } from '../utils/request-context.js';

export type TransportType = 'stdio' | 'sse' | 'http';

export function getTransportType(): TransportType {
  const transport = (process.env.TRANSPORT || 'stdio').toLowerCase();
  if (transport === 'sse' || transport === 'http') return transport;
  return 'stdio';
}

/**
 * Connect the MCP server to the selected transport.
 * Returns the HTTP server instance if using SSE/HTTP transport (for mounting API routes).
 */
export async function connectTransport(
  server: Server,
  toolCount: number,
  serverFactory?: () => Server,
): Promise<NowAIKitHttpServer | null> {
  const transportType = getTransportType();

  if (transportType === 'stdio') {
    const transport = new StdioServerTransport();
    await server.connect(transport);
    logger.info(`NowAIKit server running on stdio [${toolCount} tools]`);
    return null;
  }

  // HTTP-based transports: mount BOTH SSE and Streamable HTTP so ALL clients work regardless of protocol!
  const httpServer = createHttpServer();

  await setupSseTransport(serverFactory || (() => server), httpServer);
  await setupStreamableHttpTransport(serverFactory || (() => server), httpServer);
  addHealthRoute(httpServer);

  await httpServer.start();

  logger.info(`NowAIKit server running on sse + http [${toolCount} tools]`);
  return httpServer;
}

/**
 * SSE Transport — GET /sse opens an SSE stream, POST /messages sends client messages.
 * Uses Factory Pattern: creates an isolated Server instance for each incoming SSE connection
 * to prevent "Already connected to a transport" crashes on reconnect/multi-client.
 * Also emits a fully-qualified absolute URL in the "endpoint" event for strict clients.
 */
async function setupSseTransport(
  serverFactory: () => Server,
  httpServer: NowAIKitHttpServer,
): Promise<void> {
  const { SSEServerTransport } = await import('@modelcontextprotocol/sdk/server/sse.js');

  const sessions = new Map<string, { transport: InstanceType<typeof SSEServerTransport>; server: Server }>();

  httpServer.get('/sse', async (req, res) => {
    const host = (req.headers['x-forwarded-host'] as string) || req.headers.host || 'nowaikit-mcp.onrender.com';
    const proto = (req.headers['x-forwarded-proto'] as string) || 'https';

    // Intercept res.write to emit full absolute URL in event: endpoint
    const originalWrite = res.write.bind(res);
    res.write = function (chunk: any, encoding?: any, callback?: any): boolean {
      const str = typeof chunk === 'string' ? chunk : chunk instanceof Buffer ? chunk.toString() : '';
      if (str.includes('event: endpoint\ndata: ')) {
        const rewritten = str.replace(
          /event: endpoint\ndata: ([^\r\n]+)/,
          (_match: string, path: string) => {
            const cleanPath = path.trim();
            const abs = cleanPath.startsWith('http') ? cleanPath : `${proto}://${host}${cleanPath.startsWith('/') ? '' : '/'}${cleanPath}`;
            return `event: endpoint\ndata: ${abs}`;
          }
        );
        return originalWrite(rewritten, encoding, callback);
      }
      return originalWrite(chunk, encoding, callback);
    } as any;

    const transport = new SSEServerTransport('/messages', res);
    const sessionId = transport.sessionId;
    const sessionServer = serverFactory();
    sessions.set(sessionId, { transport, server: sessionServer });

    req.on('close', () => {
      sessions.delete(sessionId);
      logger.info(`SSE session closed: ${sessionId}`);
    });

    await sessionServer.connect(transport);
    logger.info(`SSE session connected: ${sessionId} (endpoint: ${proto}://${host}/messages?sessionId=${sessionId})`);
  }, true);

  httpServer.post('/messages', async (req, res) => {
    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const sessionId = url.searchParams.get('sessionId');

    logger.info(`[MCP] POST /messages for sessionId: ${sessionId}`);

    if (!sessionId || !sessions.has(sessionId)) {
      logger.warn(`[MCP] Session not found for sessionId: ${sessionId}`);
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid or missing sessionId' }));
      return;
    }

    const { transport } = sessions.get(sessionId)!;
    const body = (req as { body?: unknown }).body;
    logger.info(`[MCP] Dispatching message to transport for session: ${sessionId}`);
    await transport.handlePostMessage(req, res, body);
  }, true);
}

/**
 * Streamable HTTP Transport — POST/GET/DELETE /mcp for all MCP communication.
 * Handles multi-session and initialization cleanly.
 */
async function setupStreamableHttpTransport(
  serverFactory: () => Server,
  httpServer: NowAIKitHttpServer,
): Promise<void> {
  const { StreamableHTTPServerTransport } = await import('@modelcontextprotocol/sdk/server/streamableHttp.js');
  const { randomUUID } = await import('crypto');

  const sessions = new Map<string, { transport: InstanceType<typeof StreamableHTTPServerTransport>; server: Server }>();

  const defaultServer = serverFactory();
  const defaultTransport = new StreamableHTTPServerTransport();
  await defaultServer.connect(defaultTransport);

  // Mount transport on /mcp
  httpServer.post('/mcp', async (req, res) => {
    const parsedBody = (req as { body?: unknown }).body;
    const sessionIdHeader = req.headers['mcp-session-id'] as string | undefined;

    const isInit = Array.isArray(parsedBody)
      ? (parsedBody as any[]).some((m) => m?.method === 'initialize')
      : (parsedBody as any)?.method === 'initialize';

    if (isInit) {
      logger.info('[MCP] Streamable HTTP initialize request received');
      const sessionServer = serverFactory();
      const sessionTransport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
      });
      await sessionServer.connect(sessionTransport);

      if (isDelegatedAuthEnabled()) {
        const ctx = parseDelegatedAuthHeaders(req.headers as Record<string, string | string[] | undefined>);
        await runWithDelegatedAuth(ctx, () => sessionTransport.handleRequest(req, res, parsedBody));
      } else {
        await sessionTransport.handleRequest(req, res, parsedBody);
      }

      if (sessionTransport.sessionId) {
        sessions.set(sessionTransport.sessionId, { transport: sessionTransport, server: sessionServer });
        logger.info(`[MCP] Streamable HTTP session registered: ${sessionTransport.sessionId}`);
      }
      return;
    }

    if (sessionIdHeader && sessions.has(sessionIdHeader)) {
      const { transport } = sessions.get(sessionIdHeader)!;
      if (isDelegatedAuthEnabled()) {
        const ctx = parseDelegatedAuthHeaders(req.headers as Record<string, string | string[] | undefined>);
        await runWithDelegatedAuth(ctx, () => transport.handleRequest(req, res, parsedBody));
      } else {
        await transport.handleRequest(req, res, parsedBody);
      }
      return;
    }

    if (isDelegatedAuthEnabled()) {
      const ctx = parseDelegatedAuthHeaders(req.headers as Record<string, string | string[] | undefined>);
      await runWithDelegatedAuth(ctx, () => defaultTransport.handleRequest(req, res, parsedBody));
    } else {
      await defaultTransport.handleRequest(req, res, parsedBody);
    }
  }, true);

  httpServer.get('/mcp', async (req, res) => {
    const sessionIdHeader = req.headers['mcp-session-id'] as string | undefined;
    if (sessionIdHeader && sessions.has(sessionIdHeader)) {
      await sessions.get(sessionIdHeader)!.transport.handleRequest(req, res);
    } else {
      await defaultTransport.handleRequest(req, res);
    }
  }, true);

  httpServer.delete('/mcp', async (req, res) => {
    const sessionIdHeader = req.headers['mcp-session-id'] as string | undefined;
    if (sessionIdHeader && sessions.has(sessionIdHeader)) {
      await sessions.get(sessionIdHeader)!.transport.handleRequest(req, res);
      sessions.delete(sessionIdHeader);
    } else {
      await defaultTransport.handleRequest(req, res);
    }
  }, true);
}

/** Shared health endpoint — no auth required. */
function addHealthRoute(httpServer: NowAIKitHttpServer): void {
  httpServer.get('/health', async (_req, res) => {
    const { getTools } = await import('../tools/index.js');
    const tools = getTools();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      name: SERVER_NAME,
      version: VERSION,
      transport: 'sse+http',
      tools_count: tools.length,
      timestamp: new Date().toISOString(),
    }));
  }, false);
}

export { NowAIKitHttpServer } from './http-server.js';
