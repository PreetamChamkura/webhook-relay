package grpcapi

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	webhookv1 "github.com/PreetamChamkura/webhook-relay/gen/webhook/v1"
	"github.com/PreetamChamkura/webhook-relay/internal/service"
)

type Server struct {
	webhookv1.UnimplementedEventServiceServer
	Pub *service.Publisher
}

func (s *Server) Publish(ctx context.Context, req *webhookv1.PublishRequest) (*webhookv1.PublishResponse, error) {
	id, dup, err := s.Pub.Publish(ctx, "grpc", req.GetEventType(), req.GetPayloadJson(), req.GetIdempotencyKey())
	if errors.Is(err, service.ErrInvalid) {
		return nil, status.Error(codes.InvalidArgument, "event_type required and payload_json must be valid JSON")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &webhookv1.PublishResponse{EventId: id, Duplicate: dup}, nil
}
