import { eq } from "drizzle-orm";
import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { auth } from "../auth";
import db from "../database";
import { projectTable, workspaceUserTable } from "../database/schema";
import { type EventPayload, subscribeToEvent } from "../events";

// Events to broadcast via SSE
const SSE_EVENTS = [
  "task.status_changed",
  "task.assignee_changed",
  "task.priority_changed",
  "task.due_date_changed",
  "task.title_changed",
  "task.description_changed",
  "task.created",
  "task.deleted",
  "task.unassigned",
] as const;

type SSEClient = {
  id: string;
  userId: string;
  workspaceIds: Set<string>;
  send: (event: string, data: unknown) => void;
};

// Track connected clients
const clients = new Map<string, SSEClient>();

// Cache project -> workspace mapping to avoid repeated DB lookups
const projectWorkspaceCache = new Map<string, string>();

async function getWorkspaceForProject(
  projectId: string,
): Promise<string | null> {
  const cached = projectWorkspaceCache.get(projectId);
  if (cached) return cached;

  const project = await db.query.projectTable.findFirst({
    where: eq(projectTable.id, projectId),
    columns: { workspaceId: true },
  });

  if (project?.workspaceId) {
    projectWorkspaceCache.set(projectId, project.workspaceId);
    return project.workspaceId;
  }

  return null;
}

// Broadcast event to relevant clients
async function broadcastEvent(eventType: string, payload: EventPayload) {
  const data = payload.data as {
    projectId?: string;
    taskId?: string;
    workspaceId?: string;
  };

  if (!data.projectId) return;

  const workspaceId = await getWorkspaceForProject(data.projectId);
  if (!workspaceId) return;

  const eventData = {
    type: eventType,
    projectId: data.projectId,
    taskId: data.taskId,
    timestamp: payload.timestamp,
  };

  for (const client of clients.values()) {
    if (client.workspaceIds.has(workspaceId)) {
      try {
        client.send(eventType, eventData);
      } catch (error) {
        console.error(`Failed to send SSE to client ${client.id}:`, error);
      }
    }
  }
}

// Subscribe to all task events and broadcast to connected clients
for (const eventType of SSE_EVENTS) {
  subscribeToEvent(eventType, async (data) => {
    await broadcastEvent(eventType, {
      type: eventType,
      data,
      timestamp: new Date().toISOString(),
    });
  });
}

const sse = new Hono();

sse.get("/events", async (c) => {
  // Authenticate via session cookie
  const session = await auth.api.getSession({ headers: c.req.raw.headers });

  if (!session?.user) {
    return c.json({ error: "Unauthorized" }, 401);
  }

  const userId = session.user.id;

  // Get user's workspace memberships for event filtering
  const memberships = await db
    .select({ workspaceId: workspaceUserTable.workspaceId })
    .from(workspaceUserTable)
    .where(eq(workspaceUserTable.userId, userId));

  const workspaceIds = new Set(memberships.map((m) => m.workspaceId));

  return streamSSE(c, async (stream) => {
    const clientId = `${userId}-${Date.now()}`;

    // Create send function for this client
    const send = (event: string, data: unknown) => {
      stream.writeSSE({
        event,
        data: JSON.stringify(data),
        id: Date.now().toString(),
      });
    };

    // Register client
    const client: SSEClient = {
      id: clientId,
      userId,
      workspaceIds,
      send,
    };
    clients.set(clientId, client);

    console.log(
      `SSE client connected: ${clientId} (workspaces: ${workspaceIds.size})`,
    );

    // Send connection confirmation
    await stream.writeSSE({
      event: "connected",
      data: JSON.stringify({
        clientId,
        workspaceCount: workspaceIds.size,
      }),
    });

    // Keep connection alive with heartbeat every 30 seconds
    const heartbeat = setInterval(() => {
      try {
        stream.writeSSE({ event: "heartbeat", data: "" });
      } catch {
        clearInterval(heartbeat);
      }
    }, 30000);

    // Cleanup on disconnect
    stream.onAbort(() => {
      clearInterval(heartbeat);
      clients.delete(clientId);
      console.log(`SSE client disconnected: ${clientId}`);
    });

    // Keep stream open indefinitely
    await new Promise(() => {});
  });
});

export default sse;
