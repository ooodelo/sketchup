# frozen_string_literal: true

module PointCloudImporter
  # Lightweight array-like container that stores data in fixed-size chunks to
  # avoid massive contiguous allocations while still providing familiar Array
  # semantics for indexing and iteration.
  class ChunkedArray
    include Enumerable

    DEFAULT_CHUNK_CAPACITY = 100_000

    attr_reader :chunk_capacity

    def initialize(chunk_capacity = DEFAULT_CHUNK_CAPACITY)
      @chunk_capacity = [chunk_capacity.to_i, 1].max
      @chunks = []
      @length = 0
    end

    def each
      return enum_for(:each) unless block_given?

      @chunks.each do |chunk|
        chunk.each { |element| yield(element) }
      end
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
        return
      end

      values.each_slice(@chunk_capacity) do |slice|
        append_chunk(slice)
      end
    end

    def clear
      @chunks.clear
      @length = 0
    end

    def chunks
      @chunks
    end

    private

    def resolve_index(index)
      return nil unless index.is_a?(Integer)

      candidate = index
      candidate += @length if candidate.negative?
      return nil if candidate.negative? || candidate >= @length

      candidate
    end
  end
end
