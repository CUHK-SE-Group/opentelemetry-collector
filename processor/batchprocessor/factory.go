// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package batchprocessor // import "go.opentelemetry.io/collector/processor/batchprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/xprocessor"
)

const (
	defaultSendBatchSize = uint32(8192)
	defaultTimeout       = 200 * time.Millisecond

	// defaultMetadataCardinalityLimit should be set to the number
	// of metadata configurations the user expects to submit to
	// the collector.
	defaultMetadataCardinalityLimit = 1000
)

type factory struct{}

// NewFactory returns a new factory for the Batch processor.
func NewFactory() processor.Factory {
	f := &factory{}
	return xprocessor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xprocessor.WithTraces(f.createTraces, metadata.TracesStability),
		xprocessor.WithMetrics(f.createMetrics, metadata.MetricsStability),
		xprocessor.WithLogs(f.createLogs, metadata.LogsStability),
		xprocessor.WithProfiles(f.createProfiles, metadata.ProfilesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		SendBatchSize:            defaultSendBatchSize,
		Timeout:                  defaultTimeout,
		MetadataCardinalityLimit: defaultMetadataCardinalityLimit,
	}
}

func (f *factory) createTraces(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	return newTracesBatchProcessor(set, nextConsumer, cfg.(*Config))
}

func (f *factory) createMetrics(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	return newMetricsBatchProcessor(set, nextConsumer, cfg.(*Config))
}

func (f *factory) createLogs(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	return newLogsBatchProcessor(set, nextConsumer, cfg.(*Config))
}

func (f *factory) createProfiles(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer xconsumer.Profiles,
) (xprocessor.Profiles, error) {
	return newProfilesBatchProcessor(set, nextConsumer, cfg.(*Config))
}
