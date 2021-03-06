// Package udp provides the UDP input service for InfluxDB.
package udp // import "github.com/ayang64/reflux/services/udp"

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayang64/reflux/logger"
	"github.com/ayang64/reflux/models"
	"github.com/ayang64/reflux/services/meta"
	"github.com/ayang64/reflux/tsdb"
	"go.uber.org/zap"
)

const (
	// Arbitrary, testing indicated that this doesn't typically get over 10
	parserChanLen = 1000

	// MaxUDPPayload is largest payload size the UDP service will accept.
	MaxUDPPayload = 64 * 1024
)

// statistics gathered by the UDP package.
const (
	statPointsReceived      = "pointsRx"
	statBytesReceived       = "bytesRx"
	statPointsParseFail     = "pointsParseFail"
	statReadFail            = "readFail"
	statBatchesTransmitted  = "batchesTx"
	statPointsTransmitted   = "pointsTx"
	statBatchesTransmitFail = "batchesTxFail"
)

// Service is a UDP service that will listen for incoming packets of line protocol.
type Service struct {
	conn *net.UDPConn
	addr *net.UDPAddr

	mu    sync.RWMutex
	ready bool // Has the required database been created?

	parserChan chan []byte
	batcher    *tsdb.PointBatcher
	config     Config

	PointsWriter interface {
		WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	MetaClient interface {
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
	}

	Logger      *zap.Logger
	stats       *Statistics
	defaultTags models.StatisticTags
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	d := *c.WithDefaults()
	return &Service{
		config:      d,
		parserChan:  make(chan []byte, parserChanLen),
		Logger:      zap.NewNop(),
		stats:       &Statistics{},
		defaultTags: models.StatisticTags{"bind": d.BindAddress},
	}
}

// Open starts the service.
func (s *Service) Start(ctx context.Context) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.config.BindAddress == "" {
		return errors.New("bind address has to be specified in config")
	}
	if s.config.Database == "" {
		return errors.New("database has to be specified in config")
	}

	s.addr, err = net.ResolveUDPAddr("udp", s.config.BindAddress)
	if err != nil {
		s.Logger.Info("Failed to resolve UDP address",
			zap.String("bind_address", s.config.BindAddress), zap.Error(err))
		return err
	}

	s.conn, err = net.ListenUDP("udp", s.addr)
	if err != nil {
		s.Logger.Info("Failed to set up UDP listener",
			zap.Stringer("addr", s.addr), zap.Error(err))
		return err
	}

	if s.config.ReadBuffer != 0 {
		err = s.conn.SetReadBuffer(s.config.ReadBuffer)
		if err != nil {
			s.Logger.Info("Failed to set UDP read buffer",
				zap.Int("buffer_size", s.config.ReadBuffer), zap.Error(err))
			return err
		}
	}
	s.batcher = tsdb.NewPointBatcher(s.config.BatchSize, s.config.BatchPending, time.Duration(s.config.BatchTimeout))
	s.batcher.Start()

	s.Logger.Info("Started listening on UDP", zap.String("addr", s.config.BindAddress))

	go s.serve(ctx)
	go s.parser(ctx)
	go s.writer(ctx)

	<-ctx.Done()

	if s.batcher != nil {
		s.batcher.Stop()
	}

	return nil
}

// Statistics maintains statistics for the UDP service.
type Statistics struct {
	PointsReceived      int64
	BytesReceived       int64
	PointsParseFail     int64
	ReadFail            int64
	BatchesTransmitted  int64
	PointsTransmitted   int64
	BatchesTransmitFail int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "udp",
		Tags: s.defaultTags.Merge(tags),
		Values: map[string]interface{}{
			statPointsReceived:      atomic.LoadInt64(&s.stats.PointsReceived),
			statBytesReceived:       atomic.LoadInt64(&s.stats.BytesReceived),
			statPointsParseFail:     atomic.LoadInt64(&s.stats.PointsParseFail),
			statReadFail:            atomic.LoadInt64(&s.stats.ReadFail),
			statBatchesTransmitted:  atomic.LoadInt64(&s.stats.BatchesTransmitted),
			statPointsTransmitted:   atomic.LoadInt64(&s.stats.PointsTransmitted),
			statBatchesTransmitFail: atomic.LoadInt64(&s.stats.BatchesTransmitFail),
		},
	}}
}

func (s *Service) writer(ctx context.Context) {
	for {
		select {
		case batch := <-s.batcher.Out():
			// Will attempt to create database if not yet created.
			if err := s.createInternalStorage(); err != nil {
				s.Logger.Info("Required database does not yet exist",
					logger.Database(s.config.Database), zap.Error(err))
				continue
			}

			if err := s.PointsWriter.WritePointsPrivileged(s.config.Database, s.config.RetentionPolicy, models.ConsistencyLevelAny, batch); err == nil {
				atomic.AddInt64(&s.stats.BatchesTransmitted, 1)
				atomic.AddInt64(&s.stats.PointsTransmitted, int64(len(batch)))
			} else {
				s.Logger.Info("Failed to write point batch to database",
					logger.Database(s.config.Database), zap.Error(err))
				atomic.AddInt64(&s.stats.BatchesTransmitFail, 1)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) serve(ctx context.Context) {
	buf := make([]byte, MaxUDPPayload)
	for {
		select {
		case <-ctx.Done():
			// We closed the connection, time to go.
			return
		default:
			// Keep processing.
			n, _, err := s.conn.ReadFromUDP(buf)
			if err != nil {
				atomic.AddInt64(&s.stats.ReadFail, 1)
				s.Logger.Info("Failed to read UDP message", zap.Error(err))
				continue
			}
			atomic.AddInt64(&s.stats.BytesReceived, int64(n))

			bufCopy := make([]byte, n)
			copy(bufCopy, buf[:n])
			s.parserChan <- bufCopy
		}
	}
}

func (s *Service) parser(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case buf := <-s.parserChan:
			points, err := models.ParsePointsWithPrecision(buf, time.Now().UTC(), s.config.Precision)
			if err != nil {
				atomic.AddInt64(&s.stats.PointsParseFail, 1)
				s.Logger.Info("Failed to parse points", zap.Error(err))
				continue
			}

			for _, point := range points {
				s.batcher.In() <- point
			}
			atomic.AddInt64(&s.stats.PointsReceived, int64(len(points)))
		}
	}
}

// createInternalStorage ensures that the required database has been created.
func (s *Service) createInternalStorage() error {
	s.mu.RLock()
	ready := s.ready
	s.mu.RUnlock()
	if ready {
		return nil
	}

	if _, err := s.MetaClient.CreateDatabase(s.config.Database); err != nil {
		return err
	}

	// The service is now ready.
	s.mu.Lock()
	s.ready = true
	s.mu.Unlock()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "udp"))
}

// Addr returns the listener's address.
func (s *Service) Addr() net.Addr {
	return s.addr
}
