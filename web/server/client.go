// Package server provides the web UI backend
package server

import (
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const allowedOriginsEnvVar = "THREAD_POOL_ALLOWED_ORIGINS"

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     checkWebSocketOrigin,
}

func checkWebSocketOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}

	originURL, err := url.Parse(origin)
	if err != nil || originURL.Scheme == "" || originURL.Host == "" {
		return false
	}

	if strings.EqualFold(originURL.Host, r.Host) {
		return true
	}

	allowedOrigin := strings.ToLower(originURL.Scheme + "://" + originURL.Host)
	for _, configuredOrigin := range strings.Split(os.Getenv(allowedOriginsEnvVar), ",") {
		configuredOrigin = strings.TrimSpace(configuredOrigin)
		if configuredOrigin == "" {
			continue
		}

		if strings.ToLower(configuredOrigin) == allowedOrigin {
			return true
		}
	}

	return false
}

// Client is a middleman between the websocket connection and the server.
type Client struct {
	server    *Server
	conn      *websocket.Conn
	send      chan interface{} // Can be *PoolState or *TaskEvent
	requestID string
	closeOnce sync.Once
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.send)
		_ = c.conn.Close()
	})
}

// readPump pumps messages from the websocket connection to the server.
func (c *Client) readPump() {
	defer func() {
		select {
		case c.server.unregister <- c:
		case <-c.server.shutdownCh:
		}
		c.close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		// The client doesn't send any messages, so we just read to keep the connection alive
		// and handle control messages (pings/pongs/close).
		if _, _, err := c.conn.NextReader(); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				slog.Warn("WebSocket unexpected close error", "err", err, "component", "client", "request_id", c.requestID)
			}
			break
		}
	}
}

// writePump pumps messages from the server to the websocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.close()
	}()
	for {
		select {
		case state, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The server closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteJSON(state); err != nil {
				slog.Error("Failed to write JSON to WebSocket", "err", err, "component", "client", "request_id", c.requestID)
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ServeWs handles websocket requests from the peer.
func ServeWs(server *Server, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Failed to upgrade WebSocket", "err", err, "component", "server")
		return
	}
	client := &Client{server: server, conn: conn, send: make(chan interface{}, 256), requestID: requestIDFromContext(r.Context())}
	slog.Info("websocket_client_connected", "component", "server", "request_id", client.requestID, "remote_addr", r.RemoteAddr)
	select {
	case client.server.register <- client:
	case <-client.server.shutdownCh:
		client.close()
		return
	}

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	go client.writePump()
	go client.readPump()
}
