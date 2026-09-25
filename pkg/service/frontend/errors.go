package frontend

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.atoms.co/splitter/pkg/model"
)

// processLeaderError converts leader ownership errors to Unavailable for external requests.
// Internal forwarding retains ownership errors for retries.
func processLeaderError(err error) error {
	if oerr, _ := model.OwnershipErrorFromGRPC(err); model.IsOwnershipError(oerr) {
		return status.Error(codes.Unavailable, "unable to reach Splitter leader")
	}
	return model.ToGRPCError(err)
}

// processCoordinatorError converts coordinator ownership errors to Unavailable for external requests.
// Internal forwarding retains ownership errors for retries.
func processCoordinatorError(err error) error {
	if oerr, _ := model.OwnershipErrorFromGRPC(err); model.IsOwnershipError(oerr) {
		return status.Error(codes.Unavailable, "unable to reach Splitter coordinator")
	}
	return model.ToGRPCError(err)
}
