# frozen_string_literal: true

require 'minitest/autorun'
require 'tempfile'

require_relative '../point_cloud_importer/ply_parser'

module PointCloudImporter
  class PlyParserBinaryTest < Minitest::Test
    def setup
      @tempfiles = []
    end

    def teardown
      @tempfiles.each do |file|
        file.close
        file.unlink
      rescue StandardError
        # ignore cleanup issues
      end
    end

    def test_binary_parser_respects_buffer_and_chunk_size
      vertex_count = 75_000
      vertices = Array.new(vertex_count) do |index|
        [
          index.to_f,
          index.to_f + 0.5,
          index.to_f + 1.0,
          (index % 256),
          ((index + 1) % 256),
          ((index + 2) % 256),
          index.to_f / 10.0
        ]
      end
      path = write_binary_ply(vertices)

      max_read_length = wrap_file_open(path) do
        parser = PlyParser.new(path)
        chunks = []
        totals = { points: 0, colors: 0, intensities: 0 }

        parser.parse(chunk_size: 60_000) do |points, colors, intensities, processed|
          chunks << points.length
          totals[:points] += points.length
          totals[:colors] += (colors&.length || 0)
          totals[:intensities] += (intensities&.length || 0)

          assert_equal processed, totals[:points]
          refute_nil colors
          refute_nil intensities
          assert_equal points.length, colors.length
          assert_equal points.length, intensities.length
        end

        assert chunks.all? { |size| size <= 60_000 }
        assert_equal vertex_count, totals[:points]
        assert_equal vertex_count, totals[:colors]
        assert_equal vertex_count, totals[:intensities]
      end

      assert max_read_length.positive?, 'expected to capture read calls'
      assert max_read_length <= PlyParser::BINARY_READ_BUFFER_SIZE
    end

    def test_thread_yields_without_affecting_output
      vertex_count = (PlyParser::THREAD_YIELD_INTERVAL * 2) + 5
      vertices = Array.new(vertex_count) do |index|
        [
          index.to_f,
          index.to_f + 1.0,
          index.to_f + 2.0,
          10,
          20,
          30,
          (index.to_f / 2.0)
        ]
      end
      path = write_binary_ply(vertices)

      thread_singleton = class << Thread; self; end
      thread_singleton.class_eval do
        alias_method :__ply_parser_original_pass, :pass
      end
      calls = 0
      thread_singleton.class_eval do
        define_method(:pass) do
          calls += 1
          __ply_parser_original_pass()
        end
      end

      parser = PlyParser.new(path)
      points, colors, intensities, = parser.parse(chunk_size: 5_000)

      assert calls >= 2, 'expected Thread.pass to be called at least twice'
      assert_equal vertex_count, points.length
      assert_equal vertex_count, colors.length
      assert_equal vertex_count, intensities.length
      assert_equal [0.0, 1.0, 2.0], points.first
      assert_equal [10, 20, 30], colors.first
      assert_in_delta 0.0, intensities.first
      assert_equal [vertex_count - 1, vertex_count, vertex_count + 1], points.last

    ensure
      thread_singleton.class_eval do
        remove_method :pass
        alias_method :pass, :__ply_parser_original_pass
        remove_method :__ply_parser_original_pass
      end
    end

    private

    def write_binary_ply(vertices)
      tempfile = Tempfile.new(['ply_parser_binary_test', '.ply'])
      tempfile.binmode
      header = <<~PLY
        ply
        format binary_little_endian 1.0
        element vertex #{vertices.length}
        property float x
        property float y
        property float z
        property uchar red
        property uchar green
        property uchar blue
        property float intensity
        end_header
      PLY
      tempfile.write(header)

      vertices.each do |vertex|
        x, y, z, r, g, b, intensity = vertex
        tempfile.write([x, y, z].pack('e3'))
        tempfile.write([r, g, b].pack('C3'))
        tempfile.write([intensity].pack('e'))
      end
      tempfile.flush
      @tempfiles << tempfile
      tempfile.path
    end

    def wrap_file_open(target_path)
      file_singleton = class << File; self; end
      file_singleton.class_eval do
        alias_method :__ply_parser_original_open, :open
      end

      max_read_length = 0
      file_singleton.class_eval do
        define_method(:open) do |*args, &block|
          if args[0] == target_path && args[1] == 'rb' && block
            __ply_parser_original_open(*args) do |io|
              spy = ReadSpy.new(io) do |length|
                max_read_length = [max_read_length, length.to_i].max
              end
              block.call(spy)
            end
          else
            __ply_parser_original_open(*args, &block)
          end
        end
      end

      yield
      max_read_length
    ensure
      file_singleton.class_eval do
        remove_method :open
        alias_method :open, :__ply_parser_original_open
        remove_method :__ply_parser_original_open
      end
    end

    class ReadSpy
      def initialize(io, &observer)
        @io = io
        @observer = observer
      end

      def read(length = nil, *args)
        @observer&.call(length) if length
        @io.read(length, *args)
      end

      def method_missing(method_name, *args, &block)
        if @io.respond_to?(method_name)
          @io.public_send(method_name, *args, &block)
        else
          super
        end
      end

      def respond_to_missing?(method_name, include_private = false)
        @io.respond_to?(method_name, include_private) || super
      end
    end
  end
end
