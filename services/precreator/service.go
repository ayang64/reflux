// Package precreator provides the shard precreation service.
package precreator // import "github.com/ayang64/reflux/services/precreator"

import (
	"context"
	"time"

	"github.com/ayang64/reflux/logger"
	"go.uber.org/zap"
)

// Service manages the shard precreation service.
type Service struct {
	checkInterval time.Duration
	advancePeriod time.Duration

	Logger *zap.Logger

	MetaClient interface {
		PrecreateShardGroups(now, cutoff time.Time) error
	}
}

// NewService returns an instance of the precreation service.
func NewService(c Config) *Service {
	return &Service{
		checkInterval: time.Duration(c.CheckInterval),
		advancePeriod: time.Duration(c.AdvancePeriod),
		Logger:        zap.NewNop(),
	}
}

// WithLogger sets the logger for the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "shard-precreation"))
}

// Open starts the precreation service.
func (s *Service) Start(ctx context.Context) error {
	s.Logger.Info("Starting precreation service",
		logger.DurationLiteral("check_interval", s.checkInterval),
		logger.DurationLiteral("advance_period", s.advancePeriod))

	go s.runPrecreation(ctx)

	<-ctx.Done()

	return ctx.Err()
}

// runPrecreation continually checks if resources need precreation.
func (s *Service) runPrecreation(ctx context.Context) {
	for {
		select {
		case <-time.After(s.checkInterval):
			if err := s.precreate(time.Now().UTC()); err != nil {
				s.Logger.Info("Failed to precreate shards", zap.Error(err))
			}
		case <-ctx.Done():
			s.Logger.Info("Terminating precreation service")
			return
		}
	}
}

// precreate performs actual resource precreation.
func (s *Service) precreate(now time.Time) error {
	cutoff := now.Add(s.advancePeriod).UTC()
	return s.MetaClient.PrecreateShardGroups(now, cutoff)
}
