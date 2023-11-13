package core

import (
	"context"
	"go.atoms.co/lib/log"
	"go.atoms.co/splitter/pkg/model"
)

func NewServiceContext(ctx context.Context, name model.QualifiedServiceName) context.Context {
	return log.NewContext(ctx,
		log.String("tenant", name.Tenant),
		log.String("service", name.Service),
	)
}
