# encoding: utf-8
# frozen_string_literal: true

require 'minitest/autorun'

require_relative '../point_cloud_importer/chunked_array'

module PointCloudImporter
  class ChunkedArrayTest < Minitest::Test
    def test_append_chunk_reuses_array_within_capacity
      chunk_capacity = 5
      array = ChunkedArray.new(chunk_capacity)
      values = Array.new(chunk_capacity) { |index| [index, index + 0.5, index + 1.0] }

      array.append_chunk(values)

      assert_equal chunk_capacity, array.length
      assert_equal 1, array.chunks.length
      assert_same values, array.chunks.first
    end

    def test_append_chunk_slices_oversized_array
      chunk_capacity = 3
      array = ChunkedArray.new(chunk_capacity)
      values = (0...8).map { |index| [index, index + 0.1, index + 0.2] }

      array.append_chunk(values)

      assert_equal values.length, array.length
      assert_equal [3, 3, 2], array.chunks.map(&:length)
      refute_same values, array.chunks.first
      combined = array.chunks.flatten(1)
      assert_equal values, combined
    end

    def test_append_direct_allocates_new_chunks
      chunk_capacity = 3
      array = ChunkedArray.new(chunk_capacity)

      5.times { |index| array.append_direct!(index) }

      assert_equal 5, array.length
      assert_equal 2, array.chunks.length
      assert_equal [0, 1, 2], array.chunks.first
      assert_equal [3, 4], array.chunks.last
    end

    def test_trim_last_chunk_removes_unused_tail
      chunk_capacity = 4
      array = ChunkedArray.new(chunk_capacity)

      6.times { |index| array.append_direct!(index) }
      array.trim_last_chunk!

      assert_equal 6, array.length
      assert_equal 2, array.chunks.length
      assert_equal [0, 1, 2, 3], array.chunks.first
      assert_equal [4, 5], array.chunks.last
      assert_equal 2, array.chunks.last.length
    end
  end
end
