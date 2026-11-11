// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fileexporter

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter/internal/metadata"
	"strconv"
)

type TestParams map[string]any
type TestConfig struct {
	name         string
	values       TestParams
	wantInterval time.Duration
	wantErr      string `default:""`
}

func DefaultTestParams(interval int) TestParams {
	return TestParams{
		"path":          "logs.pb",
		"sync_interval": strconv.Itoa(interval) + "s",
	}
}

func CreateTestConfig(name string, extraParams map[string]any, interval int, wantErr string) TestConfig {
	params := DefaultTestParams(interval)
	if len(extraParams) > 0 {
		for key, value := range extraParams {
			params[key] = value
		}
	}
	config := TestConfig{
		name:         name,
		values:       params,
		wantInterval: time.Duration(interval) * time.Second,
		wantErr:      wantErr,
	}

	return config
}

func TestSyncIntervalConfig(t *testing.T) {
	rotationParam := map[string]any{"rotation": map[string]any{}}
	for _, tt := range []TestConfig{
		CreateTestConfig("positive sync interval", nil, 5, ""),
		CreateTestConfig("disabled sync", nil, 0, ""),
		CreateTestConfig("negative sync interval", nil, 0, ""),
		CreateTestConfig("rotation with sync", rotationParam, 1, "sync_interval is not supported with rotation"),
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			require.NoError(t, confmap.NewFromStringMap(tt.values).Unmarshal(cfg))
			err := cfg.Validate()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantInterval, cfg.SyncInterval)
		})
	}
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id           component.ID
		expected     component.Config
		errorMessage string
	}{
		{
			id: component.NewIDWithName(metadata.Type, "2"),
			expected: &Config{
				Path: "./filename.json",
				Rotation: &Rotation{
					MaxMegabytes: 10,
					MaxDays:      3,
					MaxBackups:   3,
					LocalTime:    true,
				},
				FormatType:    formatTypeJSON,
				FlushInterval: time.Second,
				GroupBy: &GroupBy{
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "3"),
			expected: &Config{
				Path: "./filename",
				Rotation: &Rotation{
					MaxMegabytes: 10,
					MaxDays:      3,
					MaxBackups:   3,
					LocalTime:    true,
				},
				FormatType:    formatTypeProto,
				Compression:   compressionZSTD,
				FlushInterval: time.Second,
				GroupBy: &GroupBy{
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "zstd_with_level"),
			expected: &Config{
				Path:        "./filename",
				FormatType:  formatTypeProto,
				Compression: compressionZSTD,
				CompressionParams: configcompression.CompressionParams{
					Level: 6,
				},
				FlushInterval: time.Second,
				GroupBy: &GroupBy{
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "rotation_with_default_settings"),
			expected: &Config{
				Path:       "./foo",
				FormatType: formatTypeJSON,
				Rotation: &Rotation{
					MaxBackups: defaultMaxBackups,
				},
				FlushInterval: time.Second,
				GroupBy: &GroupBy{
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "rotation_with_custom_settings"),
			expected: &Config{
				Path: "./foo",
				Rotation: &Rotation{
					MaxMegabytes: 1234,
					MaxBackups:   defaultMaxBackups,
				},
				FormatType:    formatTypeJSON,
				FlushInterval: time.Second,
				GroupBy: &GroupBy{
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id:           component.NewIDWithName(metadata.Type, "compression_error"),
			errorMessage: "compression is not supported",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "format_error"),
			errorMessage: "format type is not supported",
		},
		{
			id: component.NewIDWithName(metadata.Type, "flush_interval_5"),
			expected: &Config{
				Path:          "./flushed",
				FlushInterval: 5,
				FormatType:    formatTypeJSON,
				GroupBy: &GroupBy{
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "flush_interval_5s"),
			expected: &Config{
				Path:          "./flushed",
				FlushInterval: 5 * time.Second,
				FormatType:    formatTypeJSON,
				GroupBy: &GroupBy{
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "flush_interval_500ms"),
			expected: &Config{
				Path:          "./flushed",
				FlushInterval: 500 * time.Millisecond,
				FormatType:    formatTypeJSON,
				GroupBy: &GroupBy{
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id:           component.NewIDWithName(metadata.Type, "flush_interval_negative_value"),
			errorMessage: "flush_interval must be larger than zero",
		},
		{
			id:           component.NewIDWithName(metadata.Type, ""),
			errorMessage: "path must be non-empty",
		},
		{
			id: component.NewIDWithName(metadata.Type, "group_by"),
			expected: &Config{
				Path:          "./group_by/*.json",
				FlushInterval: time.Second,
				FormatType:    formatTypeJSON,
				GroupBy: &GroupBy{
					Enabled:           true,
					MaxOpenFiles:      10,
					ResourceAttribute: "dummy",
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "group_by_defaults"),
			expected: &Config{
				Path:          "./group_by/*.json",
				FlushInterval: time.Second,
				FormatType:    formatTypeJSON,
				GroupBy: &GroupBy{
					Enabled:           true,
					MaxOpenFiles:      defaultMaxOpenFiles,
					ResourceAttribute: defaultResourceAttribute,
				},
			},
		},
		{
			id:           component.NewIDWithName(metadata.Type, "group_by_invalid_path"),
			errorMessage: "path must contain exactly one * when group_by is enabled",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "group_by_invalid_path2"),
			errorMessage: "path must not start with * when group_by is enabled",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "group_by_empty_resource_attribute"),
			errorMessage: "resource_attribute must not be empty when group_by is enabled",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "group_by_zero_max_open_files"),
			errorMessage: "max_open_files must be positive when group_by is enabled",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "rotation_negative_max_megabytes"),
			errorMessage: "rotation max_megabytes, max_days and max_backups must not be negative",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "rotation_negative_max_days"),
			errorMessage: "rotation max_megabytes, max_days and max_backups must not be negative",
		},
		{
			id:           component.NewIDWithName(metadata.Type, "rotation_negative_max_backups"),
			errorMessage: "rotation max_megabytes, max_days and max_backups must not be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			if tt.expected == nil {
				assert.EqualError(t, confmap.Validate(cfg), tt.errorMessage)
				return
			}

			assert.NoError(t, confmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestDirectoryPermissionsWithoutCreateDirectory(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Path:                 "./foo",
		FormatType:           formatTypeJSON,
		CreateDirectory:      false,
		DirectoryPermissions: "0755",
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directory_permissions requires create_directory")
}

func TestDirPermissions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		perms   string
		want    os.FileMode
		wantErr error
	}{
		{perms: "", want: 0o755},
		{perms: "0700", want: 0o700},
		{perms: "abc", wantErr: errInvalidOctal},
		{perms: "01777", wantErr: errInvalidPermissionBits},
	}
	for _, tt := range tests {
		t.Run(tt.perms, func(t *testing.T) {
			cfg := &Config{DirectoryPermissions: tt.perms}
			got, err := cfg.dirPermissions()
			require.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateDoesNotMutateConfig(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Path:            "./foo",
		FormatType:      formatTypeJSON,
		CreateDirectory: true,
	}
	require.NoError(t, cfg.Validate())
	assert.Empty(t, cfg.DirectoryPermissions)
}
