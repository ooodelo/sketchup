# encoding: utf-8
# frozen_string_literal: true

require 'csv'
require 'time'

module PointCloudImporter
  # Stores a rolling history of point-to-point measurements.
  class MeasurementHistory
    DEFAULT_CAPACITY = 20

    def initialize(manager, capacity: DEFAULT_CAPACITY)
      @manager = manager
      @capacity = [capacity.to_i, 1].max
      @entries = []
      @listeners = []
      @preview_index = nil
    end

    attr_reader :capacity, :preview_index

    def entries
      @entries.map(&:dup)
    end

    def entry_at(index)
      idx = normalize_index(index)
      return unless idx

      @entries[idx]
    end

    def add(entry)
      sanitized = sanitize_entry(entry)
      @entries << sanitized
      removed = trim_to_capacity
      adjust_preview_after_removal(removed)
      notify_change
      sanitized
    end

    def remove(index)
      idx = normalize_index(index)
      return unless idx

      removed = @entries.delete_at(idx)
      adjust_preview_after_delete(idx)
      notify_change
      removed
    end

    def clear
      return if @entries.empty? && @preview_index.nil?

      @entries.clear
      @preview_index = nil
      notify_change
    end

    def preview_index=(index)
      new_index = normalize_index(index)
      if index.nil? || new_index.nil?
        new_index = nil
      end
      return if @preview_index == new_index

      @preview_index = new_index
      notify_change
      @manager.view&.invalidate
    end

    def preview_entry
      return unless @preview_index

      @entries[@preview_index]
    end

    def add_listener(listener)
      return unless listener
      @listeners << listener unless @listeners.include?(listener)
    end

    def remove_listener(listener)
      @listeners.delete(listener)
    end

    def to_csv
      CSV.generate(force_quotes: true) do |csv|
        csv << %w[timestamp distance label from_x from_y from_z to_x to_y to_z]
        @entries.each do |entry|
          csv << [
            entry[:timestamp].iso8601,
            entry[:distance],
            entry[:label],
            *entry[:from].to_a,
            *entry[:to].to_a
          ]
        end
      end
    end

    private

    def sanitize_entry(entry)
      raise ArgumentError, 'Entry must be a hash' unless entry.is_a?(Hash)

      from = point_from(entry[:from])
      to = point_from(entry[:to])
      distance = entry[:distance].to_f
      distance = from.distance(to) if distance.zero?
      timestamp = normalize_timestamp(entry[:timestamp])
      label = entry[:label].to_s

      {
        from: from.freeze,
        to: to.freeze,
        distance: distance,
        timestamp: timestamp,
        label: label
      }.freeze
    end

    def point_from(value)
      unless value.respond_to?(:to_a)
        raise ArgumentError, 'Measurement points must respond to #to_a'
      end

      Geom::Point3d.new(value.to_a)
    end

    def normalize_timestamp(value)
      return Time.now if value.nil?
      return value if value.is_a?(Time)

      if value.is_a?(Numeric)
        return Time.at(value)
      end

      Time.parse(value.to_s)
    rescue ArgumentError
      Time.now
    end

    def trim_to_capacity
      removed = 0
      while @entries.length > @capacity
        @entries.shift
        removed += 1
      end
      removed
    end

    def adjust_preview_after_removal(removed)
      return if removed.zero? || @preview_index.nil?

      if @preview_index < removed
        @preview_index = nil
      else
        @preview_index -= removed
      end
      @manager.view&.invalidate
    end

    def adjust_preview_after_delete(deleted_index)
      return if @preview_index.nil?

      if deleted_index == @preview_index
        @preview_index = nil
      elsif deleted_index < @preview_index
        @preview_index -= 1
      end
      @manager.view&.invalidate
    end

    def normalize_index(index)
      return nil if index.nil?

      idx = Integer(index)
      return nil unless idx.between?(0, @entries.length - 1)

      idx
    rescue ArgumentError, TypeError
      nil
    end

    def notify_change
      @listeners.dup.each do |listener|
        begin
          listener.call
        rescue StandardError => e
          warn("[PointCloudImporter] MeasurementHistory listener failed: #{e.message}")
        end
      end
    end
  end
end
