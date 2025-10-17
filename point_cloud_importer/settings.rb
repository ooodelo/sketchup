# encoding: utf-8
# frozen_string_literal: true

require 'singleton'

require_relative 'settings_buffer'

module PointCloudImporter
  # Simple configuration holder with SketchUp preference persistence.
  class Settings
    include Singleton

    PREFERENCES_NAMESPACE = 'PointCloudImporter::Preferences'

    DEFAULTS = {
      point_size: 3,
      point_style: :square,
      max_display_points: 2_000_000,
      density: 1.0,
      sampling_target: 500_000,
      import_chunk_size: 1_000_000,
      invalidate_every_n_chunks: 25,
      yield_interval: 10_000,
      binary_buffer_size: 4_194_304,
      binary_vertex_batch_size: 8_192,
      startup_cap: 250_000,
      invalidate_interval_ms: 200,
      batch_vertices_limit: 250_000,
      chunk_capacity: 100_000,
      max_points_sampled: 100_000,
      octree_max_points_per_node: 50_000,
      octree_max_depth: 8,
      dialog_width: 420,
      dialog_height: 520,
      panel_width: 340,
      panel_height: 440,
      auto_apply_changes: true,
      color_mode: :original,
      color_gradient: :viridis,
      single_color: '#ffffff',
      build_octree_async: true,
      logging_enabled: true,
      metrics_enabled: false
    }.freeze

    attr_reader :values

    def initialize
      @values = DEFAULTS.dup
    end

    def [](key)
      values[key]
    end

    def []=(key, value)
      key = key.to_sym
      values[key] = value
      if defined?(PointCloudImporter::Config)
        PointCloudImporter::Config.apply_setting(key, values[key])
      end
      SettingsBuffer.instance.write_setting(key, value)
    end

    def load!
      stored = Sketchup.read_default(preferences_namespace, 'values')
      if stored.is_a?(Hash)
        stored.each do |key, value|
          key = key.to_sym
          next unless DEFAULTS.key?(key)

          @values[key] = normalize_value(key, value)
        end
      end

      DEFAULTS.each_key do |key|
        next unless (value = Sketchup.read_default(preferences_namespace, key.to_s))

        @values[key] = normalize_value(key, value)
      end

      if defined?(PointCloudImporter::Config)
        PointCloudImporter::Config.load_from_settings(self)
      end
    end

    def save!(keys = nil, immediate: false)
      keys = Array(keys || @values.keys).map(&:to_sym)
      if immediate
        keys.each do |key|
          next unless @values.key?(key)

          Sketchup.write_default(preferences_namespace, key.to_s, @values[key])
        end
      else
        SettingsBuffer.instance.commit!
      end
    end

    private

    def preferences_namespace
      PREFERENCES_NAMESPACE
    end

    def normalize_value(key, value)
      case key
      when :point_style
        value.to_sym
      when :color_mode, :color_gradient
        value.to_sym
      when :single_color
        value.to_s
      when :import_chunk_size, :invalidate_every_n_chunks, :yield_interval, :binary_buffer_size,
           :binary_vertex_batch_size, :startup_cap, :invalidate_interval_ms, :batch_vertices_limit,
           :chunk_capacity, :max_points_sampled, :octree_max_points_per_node, :octree_max_depth
        value.to_i
      when :dialog_width, :dialog_height, :panel_width, :panel_height, :point_size, :max_display_points
        value.to_i
      when :auto_apply_changes, :build_octree_async, :logging_enabled, :metrics_enabled
        normalize_boolean(value)
      else
        value
      end
    rescue StandardError
      DEFAULTS[key]
    end

    def normalize_boolean(value)
      case value
      when true, false
        value
      when Numeric
        !value.to_i.zero?
      when String
        stripped = value.strip.downcase
        return true if %w[true 1 yes on].include?(stripped)
        return false if %w[false 0 no off].include?(stripped)
        !stripped.empty?
      else
        !!value
      end
    rescue StandardError
      false
    end
  end
end
