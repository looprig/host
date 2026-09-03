package hostlink

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	disconnectAuthentication     uint32 = 4500
	disconnectUnsupportedVersion uint32 = 4501
)

type centrifugeServer struct {
	node    *centrifuge.Node
	handler http.Handler
}

// NewCentrifugeServer constructs and starts the embedded JSON/WebSocket
// transport. The node uses Centrifuge's in-process broker only; application
// durability remains in SessionStore rather than transport history.
func NewCentrifugeServer(config Config) (Server, error) {
	if config.Authenticator == nil {
		return nil, errors.New("hostlink: authenticator is required")
	}
	if err := config.TenantID.Validate(); err != nil {
		return nil, errors.New("hostlink: tenant ID is invalid")
	}
	if config.PingInterval < 0 || config.PongTimeout < 0 {
		return nil, errors.New("hostlink: heartbeat durations must not be negative")
	}
	if (config.PingInterval == 0) != (config.PongTimeout == 0) {
		return nil, errors.New("hostlink: ping interval and pong timeout must be configured together")
	}
	if config.PingInterval > 0 && config.PingInterval < time.Second {
		return nil, errors.New("hostlink: ping interval must be at least one second")
	}
	if config.PingInterval > 0 && config.PongTimeout > 0 && config.PongTimeout >= config.PingInterval {
		return nil, errors.New("hostlink: pong timeout must be shorter than ping interval")
	}

	registry := prometheus.NewRegistry()
	node, err := centrifuge.New(centrifuge.Config{
		Name:    "looprig-hostlink",
		Version: "v1",
		Metrics: centrifuge.MetricsConfig{RegistererGatherer: registry},
	})
	if err != nil {
		return nil, err
	}
	node.OnConnecting(func(ctx context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		if err := config.Authenticator.VerifyTenant(ctx, config.TenantID, event.Token); err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{
				Code:   disconnectAuthentication,
				Reason: "service authentication failed",
			}
		}
		var request sessionwire.VersionNegotiationRequest
		if err := json.Unmarshal(event.Data, &request); err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{
				Code:   disconnectUnsupportedVersion,
				Reason: "unsupported wire version",
			}
		}
		response, err := sessionwire.NegotiateVersion(request)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.Disconnect{
				Code:   disconnectUnsupportedVersion,
				Reason: "unsupported wire version",
			}
		}
		data, err := json.Marshal(response)
		if err != nil {
			return centrifuge.ConnectReply{}, err
		}
		reply := centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: string(config.TenantID)},
			Data:        data,
		}
		if config.PingInterval != 0 || config.PongTimeout != 0 {
			reply.PingPongConfig = &centrifuge.PingPongConfig{
				PingInterval: config.PingInterval,
				PongTimeout:  config.PongTimeout,
			}
		}
		return reply, nil
	})
	if err := node.Run(); err != nil {
		return nil, err
	}

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
		Compression: false,
	})
	server := &centrifugeServer{node: node}
	server.handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if requestsProtobuf(request) {
			http.Error(writer, "HostLink requires the JSON protocol", http.StatusBadRequest)
			return
		}
		websocketHandler.ServeHTTP(writer, request)
	})
	return server, nil
}

func (s *centrifugeServer) Handler() http.Handler { return s.handler }

func (s *centrifugeServer) Close(ctx context.Context) error { return s.node.Shutdown(ctx) }

func requestsProtobuf(request *http.Request) bool {
	if request.URL.Query().Get("format") == "protobuf" || request.URL.Query().Get("cf_protocol") == "protobuf" {
		return true
	}
	for _, offered := range strings.Split(request.Header.Get("Sec-WebSocket-Protocol"), ",") {
		if strings.TrimSpace(offered) == "centrifuge-protobuf" {
			return true
		}
	}
	return false
}
