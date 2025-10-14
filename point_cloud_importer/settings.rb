# frozen_string_literal: true

require 'singleton'

module PointCloudImporter
  # Simple configuration holder with SketchUp preference persistence.
  class Settings
    include Singleton

    DEFAULTS = {
      point_size: 3,
      point_style: :square,
      max_display_points: 2_000_000,
      density: 1.0,
      sampling_target: 500_000,
      dialog_width: 420,
      dialog_height: 520
    }.freeze

    attr_reader :values

    def initialize
      @values = DEFAULTS.dup
    end

    def [](key)
      values[key]
    end

    def []=(key, value)
      values[key] = value
    end

    def load!
      stored = Sketchup.read_default(preferences_namespace, 'values')
      return unless stored.is_a?(Hash)

      stored.each do |key, value|
        key = key.to_sym
        next unless DEFAULTS.key?(key)

        @values[key] = value
      end
    end

    def save!
      Sketchup.write_default(preferences_namespace, 'values', @values)
    end

    private

    def preferences_namespace
      'PointCloudImporter::Preferences'
    end
  end
end
