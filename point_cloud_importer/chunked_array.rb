# encoding: utf-8
# frozen_string_literal: true

require_relative 'settings'

module PointCloudImporter
  # Lightweight array-like container that stores data in fixed-size chunks to
  # avoid massive contiguous allocations while still providing familiar Array
  # semantics for indexing and iteration.
  class ChunkedArray
    include Enumerable

    DEFAULT_CHUNK_CAPACITY = Settings::DEFAULTS[:chunk_capacity]
    DEFAULT_YIELD_INTERVAL = Settings::DEFAULTS[:yield_interval]

    attr_reader :chunk_capacity

    def initialize(chunk_capacity = DEFAULT_CHUNK_CAPACITY)
      @chunk_capacity = [chunk_capacity.to_i, 1].max
      @chunks = []
      @length = 0
      @last_len = 0
      @mutable_last_chunk = false
    end

    def each
      return enum_for(:each) unless block_given?

      remaining = @length
      @chunks.each_with_index do |chunk, index|
        break if remaining <= 0

        limit = if @mutable_last_chunk && index == @chunks.length - 1
                  [@last_len, remaining].min
                else
                  [chunk.length, remaining].min
                end

        limit.times { |offset| yield(chunk[offset]) }
        remaining -= limit
      end
    end

    def each_with_yield(interval = nil)
      return enum_for(:each_with_yield, interval) unless block_given?

      step = resolve_yield_interval(interval)
      counter = 0

      each do |value|
        yield(value)
        counter += 1
        yield_to_scheduler if step.positive? && (counter % step).zero?
      end

      self
    end

    def [](index)
      resolved_index = resolve_index(index)
      return nil if resolved_index.nil?

      chunk_index, offset = resolved_index.divmod(@chunk_capacity)
      chunk = @chunks[chunk_index]
      chunk ? chunk[offset] : nil
    end

    def []=(index, value)
      resolved_index = resolve_index(index)
      raise IndexError, 'index out of bounds' if resolved_index.nil?

      chunk_index, offset = resolved_index.divmod(@chunk_capacity)
      chunk = @chunks[chunk_index]
      raise IndexError, 'index out of bounds' unless chunk

      chunk[offset] = value
    end

    def length
      @length
    end
    alias size length

    def empty?
      @length.zero?
    end

    def append_chunk(values)
      return if values.nil? || values.empty?

      if values.length <= @chunk_capacity
        @chunks << values
        @length += values.length
        @last_len = 0
        @mutable_last_chunk = false
        return
      end

      values.each_slice(@chunk_capacity) do |slice|
        append_chunk(slice)
      end
    end

    def append_direct!(value)
      chunk = if @mutable_last_chunk && @last_len < @chunk_capacity && !@chunks.empty?
                @chunks.last
              else
                new_chunk = []
                @chunks << new_chunk
                @last_len = 0
                @mutable_last_chunk = true
                new_chunk
              end

      chunk[@last_len] = value
      @last_len += 1
      @length += 1

      value
    end

    def trim_last_chunk!
      return if @chunks.empty?

      last_chunk = @chunks.last
      desired_length = if @mutable_last_chunk
                         @last_len
                       else
                         last_chunk.length
                       end

      if desired_length.zero?
        @chunks.pop
      elsif desired_length < last_chunk.length
        last_chunk.slice!(desired_length, last_chunk.length - desired_length)
      end

      @mutable_last_chunk = false
      @last_len = 0
    end

    def clear
      @chunks.clear
      @length = 0
      @last_len = 0
      @mutable_last_chunk = false
    end

    def chunks
      @chunks
    end

    private

    def resolve_yield_interval(interval)
      candidate = interval
      candidate = config_yield_interval if candidate.nil? || candidate.to_i <= 0
      candidate = DEFAULT_YIELD_INTERVAL if candidate.nil? || candidate.to_i <= 0
      value = candidate.to_i
      value = DEFAULT_YIELD_INTERVAL if value <= 0
      value
    rescue StandardError
      DEFAULT_YIELD_INTERVAL
    end

    def config_yield_interval
      return unless defined?(PointCloudImporter::Config)

      PointCloudImporter::Config.sanitize_yield_interval(PointCloudImporter::Config.yield_interval)
    rescue StandardError
      nil
    end

    def yield_to_scheduler
      Thread.pass
      sleep(0)
    rescue StandardError
      nil
    end

    def resolve_index(index)
      return nil unless index.is_a?(Integer)

      candidate = index
      candidate += @length if candidate.negative?
      return nil if candidate.negative? || candidate >= @length

      candidate
    end
  end
end
