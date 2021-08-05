package v0

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/zookie"
	"github.com/shopspring/decimal"
)

type watchServer struct {
	v0.UnimplementedWatchServiceServer

	ds  datastore.Datastore
	nsm namespace.Manager
}

// NewWatchServer creates an instance of the watch server.
func NewWatchServer(ds datastore.Datastore, nsm namespace.Manager) v0.WatchServiceServer {
	s := &watchServer{ds: ds}
	return s
}

func (ws *watchServer) Watch(req *v0.WatchRequest, stream v0.WatchService_WatchServer) error {
	err := req.Validate()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	namespaceMap := make(map[string]struct{})
	for _, ns := range req.Namespaces {
		err = ws.nsm.CheckNamespaceAndRelation(stream.Context(), ns, datastore.Ellipsis, true)
		if err != nil {
			return status.Errorf(codes.FailedPrecondition, "invalid namespace: %s", err)
		}

		namespaceMap[ns] = struct{}{}
	}
	filter := namespaceFilter{namespaces: namespaceMap}

	var afterRevision decimal.Decimal
	if req.StartRevision != nil && req.StartRevision.Token != "" {
		decodedRevision, err := zookie.DecodeRevision(req.StartRevision)
		if err != nil {
			status.Errorf(codes.InvalidArgument, "failed to decode start revision: %s", err)
		}

		afterRevision = decodedRevision
	} else {
		var err error
		afterRevision, err = ws.ds.Revision(stream.Context())
		if err != nil {
			status.Errorf(codes.Unavailable, "failed to start watch: %s", err)
		}
	}

	updates, errchan := ws.ds.Watch(stream.Context(), afterRevision)
	for {
		select {
		case update, ok := <-updates:
			if ok {
				filtered := filter.filterUpdates(update.Changes)
				if len(filtered) > 0 {
					if err := stream.Send(&v0.WatchResponse{
						Updates:     update.Changes,
						EndRevision: zookie.NewFromRevision(update.Revision),
					}); err != nil {
						return status.Errorf(codes.Canceled, "watch canceled by user: %s", err)
					}
				}
			}
		case err := <-errchan:
			switch {
			case errors.As(err, &datastore.ErrWatchCanceled{}):
				return status.Errorf(codes.Canceled, "watch canceled by user: %s", err)
			case errors.As(err, &datastore.ErrWatchDisconnected{}):
				return status.Errorf(codes.ResourceExhausted, "watch disconnected: %s", err)
			default:
				return status.Errorf(codes.Internal, "watch error: %s", err)
			}
		}
	}
}

type namespaceFilter struct {
	namespaces map[string]struct{}
}

func (nf namespaceFilter) filterUpdates(candidates []*v0.RelationTupleUpdate) []*v0.RelationTupleUpdate {
	var filtered []*v0.RelationTupleUpdate

	for _, update := range candidates {
		if _, ok := nf.namespaces[update.Tuple.ObjectAndRelation.Namespace]; ok {
			filtered = append(filtered, update)
		}
	}

	return filtered
}