# encoding: utf-8
# frozen_string_literal: true

module PointCloudImporter
  # Estimates completion using vertex counts and/or consumed bytes.
  class ProgressEstimator
    attr_reader :total_vertices, :total_bytes, :processed_vertices, :processed_bytes

    MIN_UPDATE_INTERVAL = 1.0 / 15.0

    def initialize(total_vertices: nil, total_bytes: nil)
      reset(total_vertices: total_vertices, total_bytes: total_bytes)
      @min_interval = MIN_UPDATE_INTERVAL
      @last_emit_at = -Float::INFINITY
      @last_message = nil
    end

    def reset(total_vertices: nil, total_bytes: nil)
      @total_vertices = normalize_total(total_vertices)
      @total_bytes = normalize_total(total_bytes)
      @processed_vertices = 0
      @processed_bytes = 0
      @last_emit_at = -Float::INFINITY
      @last_message = nil
    end

    def update_totals(total_vertices: nil, total_bytes: nil)
      if total_vertices
        normalized = normalize_total(total_vertices)
        @total_vertices = normalized if normalized.positive?
      end

      return unless total_bytes

      normalized = normalize_total(total_bytes)
      @total_bytes = normalized if normalized.positive?
    end

    def update(processed_vertices: nil, consumed_bytes: nil)
      if processed_vertices
        @processed_vertices = [processed_vertices.to_i, @processed_vertices].max
      end

      return unless consumed_bytes

      bytes = consumed_bytes.to_i
      return if bytes <= 0

      @processed_bytes += bytes
    end

    def fraction
      vertex_fraction = vertex_fraction()
      byte_fraction = byte_fraction()

      if vertex_fraction
        if @processed_vertices <= @total_vertices || byte_fraction.nil?
          vertex_fraction
        else
          byte_fraction || vertex_fraction
        end
      else
        byte_fraction || 0.0
      end
    end

    private

    def vertex_fraction
      return unless @total_vertices.positive?

      [@processed_vertices.to_f / @total_vertices, 1.0].min
    end

    def byte_fraction
      return unless @total_bytes.positive?

      [@processed_bytes.to_f / @total_bytes, 1.0].min
    end

    def normalize_total(value)
      total = value.to_i
      total.positive? ? total : 0
    end

    def ready_to_emit?(now, message:, fraction:, force: false)
      return true if force

      return true if fraction && fraction >= 1.0
      return true if message && message != @last_message

      elapsed = now - @last_emit_at
      elapsed >= @min_interval
    rescue StandardError
      true
    end

    def mark_emitted(now, message:)
      @last_emit_at = now
      @last_message = message
    end
  end
end
