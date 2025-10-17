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
      metrics_enabled: false,
      import_preset: :balanced,
      stress_reference_path: File.expand_path(File.join('..', 'docs', 'reference_cloud.ply'), __dir__),
      stress_log_path: nil,
      stress_long_phase_threshold: 5.0
    }.freeze

    IMPORT_PRESETS = {
      fast: {
        import_chunk_size: 500_000,
        invalidate_every_n_chunks: 40,
        yield_interval: 5_000,
        binary_buffer_size: 2_097_152,
        binary_vertex_batch_size: 4_096,
        density: 0.35,
        max_display_points: 1_000_000,
        sampling_target: 250_000,
        max_points_sampled: 50_000,
        octree_max_points_per_node: 80_000,
        octree_max_depth: 7
      }.freeze,
      balanced: {
        import_chunk_size: 1_000_000,
        invalidate_every_n_chunks: 25,
        yield_interval: 10_000,
        binary_buffer_size: 4_194_304,
        binary_vertex_batch_size: 8_192,
        density: 1.0,
        max_display_points: 2_000_000,
        sampling_target: 500_000,
        max_points_sampled: 100_000,
        octree_max_points_per_node: 50_000,
        octree_max_depth: 8
      }.freeze,
      quality: {
        import_chunk_size: 1_600_000,
        invalidate_every_n_chunks: 15,
        yield_interval: 8_000,
        binary_buffer_size: 6_291_456,
        binary_vertex_batch_size: 12_288,
        density: 1.0,
        max_display_points: 3_000_000,
        sampling_target: 750_000,
        max_points_sampled: 200_000,
        octree_max_points_per_node: 35_000,
        octree_max_depth: 9
      }.freeze
    }.freeze

    PRESET_CONTROLLED_KEYS = IMPORT_PRESETS.values.flat_map(&:keys).uniq.freeze

    attr_reader :values

    def initialize
      @values = DEFAULTS.dup
      @applying_preset = false
    end

    def [](key)
      values[key]
    end

    def []=(key, value)
      key = key.to_sym
      if key == :import_preset
        apply_import_preset!(value)
        return
      end

      assign_value(key, value)
      mark_preset_as_custom!(key)
    end

    def snapshot
      buffer = SettingsBuffer.instance
      buffer.snapshot(@values)
    end

    def import_preset_parameters(key = @values[:import_preset])
      preset_key = normalize_preset_key(key)
      return nil unless preset_key && IMPORT_PRESETS.key?(preset_key)

      IMPORT_PRESETS[preset_key].each_with_object({}) do |(setting_key, setting_value), memo|
        memo[setting_key] = setting_value
      end
    end

    def available_import_presets
      IMPORT_PRESETS.keys
    end

    def load!
      stored = Sketchup.read_default(preferences_namespace, 'values')
      if stored.is_a?(Hash)
        stored.each do |key, value|
          key = key.to_sym
          next unless DEFAULTS.key?(key)

          assign_value(key, value, persist: false)
        end
      end

      DEFAULTS.each_key do |key|
        next unless (value = Sketchup.read_default(preferences_namespace, key.to_s))

        assign_value(key, value, persist: false)
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
      when :density
        value.to_f
      when :import_preset
        normalize_preset_key(value) || :custom
      when :stress_reference_path
        value.to_s
      when :stress_log_path
        value.nil? ? nil : value.to_s
      when :stress_long_phase_threshold
        candidate = value.respond_to?(:to_f) ? value.to_f : Float(value)
        candidate.positive? ? candidate : DEFAULTS[:stress_long_phase_threshold]
      else
        value
      end
    rescue StandardError
      DEFAULTS[key]
    end

    def assign_value(key, value, persist: true)
      normalized = normalize_value(key, value)
      @values[key] = normalized
      if persist
        if defined?(PointCloudImporter::Config)
          PointCloudImporter::Config.apply_setting(key, normalized)
        end
        SettingsBuffer.instance.write_setting(key, normalized)
      end
      normalized
    end
    private :assign_value

    def mark_preset_as_custom!(key)
      return unless PRESET_CONTROLLED_KEYS.include?(key)
      return if @applying_preset

      assign_value(:import_preset, :custom)
    end
    private :mark_preset_as_custom!

    def apply_import_preset!(value)
      preset_key = normalize_preset_key(value)
      preset_key = :custom if preset_key.nil?

      if preset_key == :custom
        assign_value(:import_preset, :custom)
        return
      end

      preset = IMPORT_PRESETS[preset_key]
      return unless preset

      @applying_preset = true
      preset.each do |setting_key, setting_value|
        assign_value(setting_key, setting_value)
      end
      assign_value(:import_preset, preset_key)
    ensure
      @applying_preset = false
    end
    private :apply_import_preset!

    def normalize_preset_key(value)
      key =
        case value
        when nil
          nil
        when Symbol
          value
        else
          string = value.to_s.strip
          return nil if string.empty?

          string.downcase.to_sym
        end

      return key if key && (IMPORT_PRESETS.key?(key) || key == :custom)

      nil
    rescue StandardError
      nil
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
