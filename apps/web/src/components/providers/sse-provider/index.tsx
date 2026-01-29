import { useQueryClient } from "@tanstack/react-query";
import {
  createContext,
  type PropsWithChildren,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { useAuth } from "@/components/providers/auth-provider/hooks/use-auth";

type SSEContextValue = {
  isConnected: boolean;
  connectionError: string | null;
};

const SSEContext = createContext<SSEContextValue>({
  isConnected: false,
  connectionError: null,
});

export function useSSE() {
  return useContext(SSEContext);
}

type SSEEvent = {
  type: string;
  projectId?: string;
  taskId?: string;
  timestamp: string;
};

export function SSEProvider({ children }: PropsWithChildren) {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const eventSourceRef = useRef<EventSource | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout>>();
  const reconnectAttempts = useRef(0);

  useEffect(() => {
    if (!user) {
      // Clean up if user logs out
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      setIsConnected(false);
      setConnectionError(null);
      return;
    }

    function connect() {
      // Don't create multiple connections
      if (eventSourceRef.current?.readyState === EventSource.OPEN) {
        return;
      }

      const apiUrl = import.meta.env.VITE_API_URL || "http://localhost:1337";
      const eventSource = new EventSource(`${apiUrl}/api/sse/events`, {
        withCredentials: true,
      });

      eventSource.onopen = () => {
        setIsConnected(true);
        setConnectionError(null);
        reconnectAttempts.current = 0;
      };

      eventSource.onerror = () => {
        setIsConnected(false);
        setConnectionError("Connection lost");
        eventSource.close();
        eventSourceRef.current = null;

        // Exponential backoff for reconnection (max 30 seconds)
        const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 30000);
        reconnectAttempts.current++;

        reconnectTimeoutRef.current = setTimeout(connect, delay);
      };

      // Handle connection confirmation
      eventSource.addEventListener("connected", (e) => {
        console.log("SSE connected:", JSON.parse(e.data));
      });

      // Handle task events - invalidate relevant queries
      const handleTaskEvent = (e: MessageEvent) => {
        const event: SSEEvent = JSON.parse(e.data);
        invalidateTaskQueries(event);
      };

      eventSource.addEventListener("task.status_changed", handleTaskEvent);
      eventSource.addEventListener("task.assignee_changed", handleTaskEvent);
      eventSource.addEventListener("task.priority_changed", handleTaskEvent);
      eventSource.addEventListener("task.title_changed", handleTaskEvent);
      eventSource.addEventListener("task.description_changed", handleTaskEvent);
      eventSource.addEventListener("task.due_date_changed", handleTaskEvent);
      eventSource.addEventListener("task.created", handleTaskEvent);
      eventSource.addEventListener("task.deleted", handleTaskEvent);
      eventSource.addEventListener("task.unassigned", handleTaskEvent);

      eventSourceRef.current = eventSource;
    }

    function invalidateTaskQueries(event: SSEEvent) {
      // Invalidate project tasks list
      if (event.projectId) {
        queryClient.invalidateQueries({
          queryKey: ["tasks", event.projectId],
        });
      }

      // Invalidate individual task query
      if (event.taskId) {
        queryClient.invalidateQueries({
          queryKey: ["task", event.taskId],
        });
        // Also invalidate activities for this task
        queryClient.invalidateQueries({
          queryKey: ["activities", event.taskId],
        });
      }

      // Refresh notifications since task changes may create notifications
      queryClient.invalidateQueries({
        queryKey: ["notifications"],
      });
    }

    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  }, [user, queryClient]);

  return (
    <SSEContext.Provider value={{ isConnected, connectionError }}>
      {children}
    </SSEContext.Provider>
  );
}

export default SSEProvider;
