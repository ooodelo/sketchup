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
      dialog_width: 420,
      dialog_height: 520,
      auto_apply_changes: true,
      color_mode: :original,
      color_gradient: :viridis,
      single_color: '#ffffff'
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
      else
        value
      end
    rescue StandardError
      DEFAULTS[key]
    end
  end
end
