# frozen_string_literal: true

module PointCloudImporter
  # Estimates import progress based on available metadata.
  class ProgressEstimator
    def initialize(total_vertices: nil, data_bytes_total: nil)
      @total_vertices = normalize_positive_integer(total_vertices)
      @data_bytes_total = normalize_positive_integer(data_bytes_total)
    end

    def fraction(processed_vertices:, data_bytes_processed: nil)
      if @total_vertices
        ratio(processed_vertices.to_f, @total_vertices)
      elsif @data_bytes_total && data_bytes_processed
        ratio(data_bytes_processed.to_f, @data_bytes_total)
      else
        nil
      end
    end

    private

    def normalize_positive_integer(value)
      integer = value.to_i
      integer.positive? ? integer : nil
    rescue StandardError
      nil
    end

    def ratio(numerator, denominator)
      return nil unless denominator.positive?

      value = numerator / denominator
      [[value, 0.0].max, 1.0].min
    rescue ZeroDivisionError, StandardError
      nil
    end
  end
end
