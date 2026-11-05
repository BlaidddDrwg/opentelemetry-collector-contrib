// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"

	replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"
)

func loadReplayConfig(ctx context.Context, paths, overrides []string, file string) (replay.Config, error) {
	cfg := replay.DefaultConfig()
	uris := append([]string(nil), paths...)
	for _, override := range overrides {
		key, value, ok := strings.Cut(override, "=")
		if !ok || !strings.HasPrefix(key, "replay.") {
			return cfg, fmt.Errorf("replay --set requires replay.<field>=<value>: %q", override)
		}
		uris = append(uris, "yaml:"+strings.ReplaceAll(key, ".", "::")+": "+value)
	}
	resolver, err := confmap.NewResolver(confmap.ResolverSettings{
		URIs: uris,
		ProviderFactories: []confmap.ProviderFactory{
			fileprovider.NewFactory(), envprovider.NewFactory(), yamlprovider.NewFactory(),
		},
		DefaultScheme: "env",
	})
	if err != nil {
		return cfg, err
	}
	defer func() { _ = resolver.Shutdown(ctx) }()
	conf, err := resolver.Resolve(ctx)
	if err != nil {
		return cfg, err
	}
	sub, err := conf.Sub("replay")
	if err != nil {
		return cfg, err
	}
	if err = sub.Unmarshal(&cfg); err != nil {
		return cfg, fmt.Errorf("invalid replay configuration: %w", err)
	}
	if file != "" {
		cfg.File = file
	}
	return cfg, cfg.Validate()
}
