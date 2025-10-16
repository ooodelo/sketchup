# encoding: utf-8
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

    private

    def pack_color(r, g, b)
      ((r & 0xff) << 16) | ((g & 0xff) << 8) | (b & 0xff)
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

    def test_binary_parser_handles_xyz_only
      vertex_count = 1_024
      vertices = Array.new(vertex_count) do |index|
        [index.to_f, index.to_f + 0.5, index.to_f + 1.0]
      end
      path = write_binary_ply(vertices, properties: %i[x y z])

      parser = PlyParser.new(path)
      points, colors, intensities, = parser.parse(chunk_size: 500)

      assert_equal vertex_count, points.length
      assert_nil colors
      assert_nil intensities
      assert_equal [0.0, 0.5, 1.0], points.first
      assert_equal [vertex_count - 1, vertex_count - 0.5, vertex_count], points.last
    end

    def test_binary_parser_handles_xyzrgb_big_endian
      vertex_count = 2_048
      vertices = Array.new(vertex_count) do |index|
        [
          index.to_f,
          index.to_f + 1.0,
          index.to_f + 2.0,
          (index % 256),
          ((index + 1) % 256),
          ((index + 2) % 256)
        ]
      end
      path = write_binary_ply(vertices, properties: %i[x y z red green blue], format: :big)

      parser = PlyParser.new(path)
      points, colors, intensities, = parser.parse(chunk_size: 1_000)

      assert_equal vertex_count, points.length
      refute_nil colors
      assert_nil intensities
      assert_equal [0.0, 1.0, 2.0], points.first
      assert_kind_of Integer, colors.first
      assert_equal pack_color(0, 1, 2), colors.first
      assert_equal [vertex_count - 1, vertex_count, vertex_count + 1], points.last
    end

    def test_binary_parser_handles_xyz_with_intensity
      vertex_count = 3_000
      vertices = Array.new(vertex_count) do |index|
        [
          index.to_f,
          index.to_f + 0.25,
          index.to_f + 0.5,
          index.to_f / 100.0
        ]
      end
      path = write_binary_ply(vertices, properties: %i[x y z intensity])

      parser = PlyParser.new(path)
      points, colors, intensities, = parser.parse(chunk_size: 512)

      assert_equal vertex_count, points.length
      assert_nil colors
      refute_nil intensities
      assert_in_delta 0.0, intensities.first
      assert_in_delta((vertex_count - 1) / 100.0, intensities.last)
    end

    def test_binary_parser_progress_callback_respects_interval
      vertex_count = 12_500
      vertices = Array.new(vertex_count) do |index|
        [
          index.to_f,
          index.to_f + 0.5,
          index.to_f + 1.0,
          (index % 256),
          ((index + 1) % 256),
          ((index + 2) % 256)
        ]
      end
      path = write_binary_ply(vertices, properties: %i[x y z red green blue])

      progress_calls = []

      parser = PlyParser.new(path, progress_callback: lambda do |payload|
        progress_calls << payload.dup
      end)

      parser.singleton_class.class_eval do
        attr_accessor :_mock_times

        def monotonic_time
          @_mock_times ||= Array.new(50, 0.0) + Array.new(50, 1.0) + Array.new(50, 2.0)
          value = @_mock_times.shift
          value = @_mock_times.last if value.nil?
          value || 2.0
        end
      end

      parser.parse(chunk_size: 1_000)

      refute_empty progress_calls
      assert progress_calls.length <= 3, 'expected progress to be reported at most three times'
      assert_equal vertex_count, progress_calls.last[:processed_vertices]
    end

    def test_binary_parser_cancellation_respects_time_checks
      vertex_count = 10_000
      vertices = Array.new(vertex_count) do |index|
        [
          index.to_f,
          index.to_f + 0.5,
          index.to_f + 1.0,
          (index % 256),
          ((index + 1) % 256),
          ((index + 2) % 256)
        ]
      end
      path = write_binary_ply(vertices, properties: %i[x y z red green blue])

      should_cancel = false
      progress_calls = []

      parser = PlyParser.new(
        path,
        progress_callback: lambda do |payload|
          progress_calls << payload
          should_cancel = payload[:processed_vertices].positive?
        end,
        cancelled_callback: lambda do
          should_cancel
        end
      )

      parser.singleton_class.class_eval do
        attr_accessor :_mock_times

        def monotonic_time
          @_mock_times ||= Array.new(20, 0.0) + Array.new(20, 0.3) + Array.new(20, 0.8)
          value = @_mock_times.shift
          value = @_mock_times.last if value.nil?
          value || 0.8
        end
      end

      assert_raises(PlyParser::Cancelled) do
        parser.parse(chunk_size: 1_000)
      end

      assert progress_calls.any? { |payload| payload[:processed_vertices].positive? },
             'expected progress callback to receive a non-zero processed vertex count'
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
      assert_kind_of Integer, colors.first
      assert_equal pack_color(10, 20, 30), colors.first
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

    def write_binary_ply(vertices, properties: default_properties, format: :little)
      tempfile = Tempfile.new(['ply_parser_binary_test', '.ply'])
      tempfile.binmode
      format_token = format == :big ? 'binary_big_endian' : 'binary_little_endian'
      header = <<~PLY
        ply
        format #{format_token} 1.0
        element vertex #{vertices.length}
#{property_header_lines(properties).join("\n")}
        end_header
      PLY
      tempfile.write(header)

      float_directive = format == :big ? 'g' : 'e'

      vertices.each do |vertex|
        values = vertex.dup
        properties.each do |property|
          value = values.shift
          case property
          when :x, :y, :z
            tempfile.write([value].pack(float_directive))
          when :red, :green, :blue
            tempfile.write([value].pack('C'))
          when :intensity
            tempfile.write([value].pack(float_directive))
          else
            raise ArgumentError, "Unsupported property #{property.inspect}"
          end
        end
      end
      tempfile.flush
      @tempfiles << tempfile
      tempfile.path
    end

    def property_header_lines(properties)
      properties.map do |property|
        case property
        when :x, :y, :z
          "        property float #{property}"
        when :red, :green, :blue
          "        property uchar #{property}"
        when :intensity
          "        property float intensity"
        else
          raise ArgumentError, "Unsupported property #{property.inspect}"
        end
      end
    end

    def default_properties
      %i[x y z red green blue intensity]
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
