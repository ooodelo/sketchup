# encoding: utf-8
# frozen_string_literal: true

module PointCloudImporter
  # Numeric helpers shared across the importer.
  module Numbers
    module_function

    def clamp(value, min_value, max_value)
      return min_value if value.nil?

      [[value, min_value].max, max_value].min
    rescue StandardError
      min_value
    end
  end
end
