# encoding: utf-8
# frozen_string_literal: true

require 'stringio'

require_relative 'progress_estimator'

module PointCloudImporter
    # Parser for PLY point cloud files (ASCII, binary little endian or binary big endian).
  class PlyParser
    UnsupportedFormat = Class.new(StandardError)
    Cancelled = Class.new(StandardError)

    DEFAULT_CHUNK_SIZE = 100_000
    PROGRESS_REPORT_INTERVAL = 0.5
    THREAD_YIELD_INTERVAL = 10_000
    BINARY_READ_BUFFER_SIZE = 1_048_576
    BINARY_VERTEX_BATCH_MIN = 4_096
    BINARY_VERTEX_BATCH_MAX = 8_192
    DEFAULT_BINARY_VERTEX_BATCH_SIZE = BINARY_VERTEX_BATCH_MIN


    TYPE_MAP = {
      'char' => 'c',
      'uchar' => 'C',
      'int8' => 'c',
      'uint8' => 'C',
      'short' => 's<',
      'ushort' => 'S<',
      'int16' => 's<',
      'uint16' => 'S<',
      'int' => 'l<',
      'int32' => 'l<',
      'uint' => 'L<',
      'uint32' => 'L<',
      'float' => 'e',
      'float32' => 'e',
      'double' => 'E',
      'float64' => 'E',
      'char_be' => 'c',
      'uchar_be' => 'C',
      'int8_be' => 'c',
      'uint8_be' => 'C',
      'short_be' => 's>',
      'ushort_be' => 'S>',
      'int16_be' => 's>',
      'uint16_be' => 'S>',
      'int_be' => 'l>',
      'int32_be' => 'l>',
      'uint_be' => 'L>',
      'uint32_be' => 'L>',
      'float_be' => 'g',
      'float32_be' => 'g',
      'double_be' => 'G',
      'float64_be' => 'G'
    }.freeze

    RED_PROPERTY_NAMES = %w[red diffuse_red r].freeze
    GREEN_PROPERTY_NAMES = %w[green diffuse_green g].freeze
    BLUE_PROPERTY_NAMES = %w[blue diffuse_blue b].freeze
    INTENSITY_PROPERTY_NAMES = %w[intensity intensity_0 reflectance greyscale].freeze

    attr_reader :path, :total_vertex_count, :metadata, :estimated_progress, :progress_estimator

    def initialize(path, progress_callback: nil, cancelled_callback: nil)
      @path = path
      @progress_callback = progress_callback
      @cancelled_callback = cancelled_callback
      @total_vertex_count = 0
      @metadata = {}
      @last_report_time = monotonic_time - PROGRESS_REPORT_INTERVAL
      @estimated_progress = 0.0
      @progress_estimator = ProgressEstimator.new
    end

    def parse(chunk_size: nil, &block)
      config = configuration
      default_chunk_size = config&.chunk_size || DEFAULT_CHUNK_SIZE
      chunk_size = sanitize_positive_integer(chunk_size, default_chunk_size)
      chunk_size = config.sanitize_chunk_size(chunk_size) if config

      collector_points = []
      collector_colors = []
      collector_intensities = []
      has_colors = false
      has_intensity = false

      emitter = if block_given?
                  block
                else
                  lambda do |points_chunk, colors_chunk, intensities_chunk, _processed|
                    collector_points.concat(points_chunk)
                    if colors_chunk
                      has_colors = true
                      collector_colors.concat(colors_chunk)
                    end
                    if intensities_chunk
                      has_intensity = true
                      collector_intensities.concat(intensities_chunk)
                    end
                  end
                end

      File.open(path, 'rb') do |io|
        header = parse_header(io)
        @total_vertex_count = header[:vertex_count] ? header[:vertex_count].to_i : 0
        @metadata = header[:metadata] || {}

        data_bytes_total = remaining_bytes(io)
        @progress_estimator = ProgressEstimator.new(
          total_vertices: @total_vertex_count,
          total_bytes: data_bytes_total
        )
        @estimated_progress = 0.0
        @last_data_position = io.pos
        @last_report_time = monotonic_time - PROGRESS_REPORT_INTERVAL

        @format_string_cache = {}

        report_progress(force: true)

        validate_header!(header)

        case header[:format]
        when :ascii
          parse_ascii(io, header, chunk_size, &emitter)
        when :binary_little
          parse_binary(io, header, chunk_size, &emitter)
        when :binary_big
          parse_binary_big(io, header, chunk_size, &emitter)
        else
          raise UnsupportedFormat, 'Только ASCII, binary_little_endian и binary_big_endian PLY поддерживаются.'
        end
      end

      if block_given?
        @metadata
      else
        colors_result = has_colors ? collector_colors : nil
        intensities_result = has_intensity ? collector_intensities : nil
        [collector_points, colors_result, intensities_result, @metadata]
      end
    end

    private

    def cancelled?
      @cancelled_callback && @cancelled_callback.call
    rescue StandardError
      false
    end

    def check_cancelled!
      raise Cancelled if cancelled?
    end

    def parse_header(io)
      header = { properties: [], metadata: {} }
      format_line = io.gets&.strip
      raise UnsupportedFormat, 'Не PLY файл' unless format_line == 'ply'

      current_element = nil

      until (line = io.gets&.strip).nil?
        case line
        when /^format\s+(ascii|binary_little_endian|binary_big_endian)(?:\s+([\d.]+))?/i
          header[:format] =
            case Regexp.last_match(1)
            when /ascii/i then :ascii
            when /binary_little_endian/i then :binary_little
            when /binary_big_endian/i then :binary_big
            end
          version = Regexp.last_match(2)
          header[:metadata][:format_version] = version if version
        when /^comment\s+(.*)$/
          (header[:metadata][:comments] ||= []) << Regexp.last_match(1)
        when /^obj_info\s+(.*)$/
          (header[:metadata][:obj_info] ||= []) << Regexp.last_match(1)
        when /^element\s+vertex\s+(\d+)/i
          header[:vertex_count] = Regexp.last_match(1).to_i
          header[:properties] = []
          current_element = :vertex
        when /^element\s+([\w\d_]+)\s+\d+/i
          current_element = Regexp.last_match(1).casecmp('vertex').zero? ? :vertex : :other
        when /^property\s+list\b/i
          if current_element == :vertex
            raise UnsupportedFormat, 'Списковые свойства для вершин не поддерживаются.'
          end
        when /^property\s+([\w\d_]+)\s+([\w\d_]+)/i
          next unless current_element == :vertex

          property_type = Regexp.last_match(1).downcase
          property_name = Regexp.last_match(2).downcase
          position = header[:properties].length
          header[:properties] << { type: property_type, name: property_name, position: position }
        when 'end_header'
          break
        end
      end
      raise UnsupportedFormat, 'Неизвестный формат PLY.' unless header[:format]
      header[:property_index_by_name] = build_property_index(header[:properties])
      header
    end

    def parse_ascii(io, header, chunk_size, &block)
      vertex_count = header[:vertex_count] ? header[:vertex_count].to_i : 0
      property_index_by_name = header[:property_index_by_name]
      color_indices = color_property_indices(property_index_by_name)
      intensity_index = intensity_property_index(property_index_by_name)

      points_chunk = []
      colors_chunk = color_indices ? [] : nil
      intensities_chunk = intensity_index ? [] : nil
      processed = 0

      yield_interval = configuration_value(:yield_interval, THREAD_YIELD_INTERVAL)

      vertex_count.times do |index|
        check_cancelled!
        Thread.pass if yield_interval.positive? && (index % yield_interval).zero? && index.positive?
        line = io.gets
        current_pos = io.pos
        consumed_bytes = [current_pos - @last_data_position, 0].max
        @last_data_position = current_pos
        break unless line

        values = line.split
        point, color, intensity = interpret_vertex(values, property_index_by_name, color_indices, intensity_index)
        points_chunk << point
        colors_chunk << color if colors_chunk
        intensities_chunk << intensity if intensities_chunk
        processed += 1

        update_progress(processed_vertices: processed, consumed_bytes: consumed_bytes)

        next unless points_chunk.length >= chunk_size

        emit_chunk(points_chunk, colors_chunk, intensities_chunk, processed, block)
        points_chunk = []
        colors_chunk = color_indices ? [] : nil
        intensities_chunk = intensity_index ? [] : nil
      end

      emit_chunk(points_chunk, colors_chunk, intensities_chunk, processed, block)
      report_progress(force: true)
    end

    def parse_binary(io, header, chunk_size, &block)
      parse_binary_with_endian(io, header, :little, chunk_size, &block)
    end

    def parse_binary_big(io, header, chunk_size, &block)
      parse_binary_with_endian(io, header, :big, chunk_size, &block)
    end

    def parse_binary_with_endian(io, header, endian, chunk_size, &block)
      vertex_count = header[:vertex_count] ? header[:vertex_count].to_i : 0
      properties = header[:properties]
      property_index_by_name = header[:property_index_by_name]
      color_indices = color_property_indices(property_index_by_name)
      intensity_index = intensity_property_index(property_index_by_name)

      x_index = property_index_by_name['x']
      y_index = property_index_by_name['y']
      z_index = property_index_by_name['z']
      r_index, g_index, b_index = color_indices if color_indices

      stride = properties.sum { |property| bytesize(property[:type]) }
      chunk_size = [chunk_size.to_i, 1].max

      buffer_size = configuration_value(:binary_buffer_size, BINARY_READ_BUFFER_SIZE)
      max_vertices_per_buffer = [buffer_size / stride, 1].max

      batch_size_preference = configuration_value(:binary_vertex_batch_size,
                                                  DEFAULT_BINARY_VERTEX_BATCH_SIZE)
      batch_size_preference = batch_size_preference.to_i
      if batch_size_preference < BINARY_VERTEX_BATCH_MIN
        batch_size_preference = BINARY_VERTEX_BATCH_MIN
      elsif batch_size_preference > BINARY_VERTEX_BATCH_MAX
        batch_size_preference = BINARY_VERTEX_BATCH_MAX
      end

      yield_interval = configuration_value(:yield_interval, THREAD_YIELD_INTERVAL)

      processed = 0
      property_count = properties.length

      points_chunk = []
      colors_chunk = color_indices ? [] : nil
      intensities_chunk = intensity_index ? [] : nil

      while processed < vertex_count
        check_cancelled!

        remaining = vertex_count - processed
        batch_size = [batch_size_preference, max_vertices_per_buffer, remaining, BINARY_VERTEX_BATCH_MAX].min
        bytes_to_read = stride * batch_size
        raw_data = io.read(bytes_to_read)
        break unless raw_data && raw_data.bytesize == bytes_to_read

        flat_values = raw_data.unpack(
          build_format_string(properties, endian: endian, batch_size: batch_size)
        )

        batch_size.times do |index|
          base_offset = index * property_count

          x = flat_values[base_offset + x_index].to_f
          y = flat_values[base_offset + y_index].to_f
          z = flat_values[base_offset + z_index].to_f
          points_chunk << [x, y, z]

          if colors_chunk
            r = flat_values[base_offset + r_index].to_i & 0xff
            g = flat_values[base_offset + g_index].to_i & 0xff
            b = flat_values[base_offset + b_index].to_i & 0xff
            colors_chunk << ((r << 16) | (g << 8) | b)
          end

          intensities_chunk << flat_values[base_offset + intensity_index] if intensities_chunk

          processed += 1

          Thread.pass if yield_interval.positive? && (processed % yield_interval).zero? && processed.positive?

          next unless points_chunk.length >= chunk_size

          emit_chunk(points_chunk, colors_chunk, intensities_chunk, processed, block)
          points_chunk = []
          colors_chunk = color_indices ? [] : nil
          intensities_chunk = intensity_index ? [] : nil
        end

        update_progress(processed_vertices: processed, consumed_bytes: bytes_to_read)
      end

      emit_chunk(points_chunk, colors_chunk, intensities_chunk, processed, block)
      report_progress(force: true)
    end


    def emit_chunk(points_chunk, colors_chunk, intensities_chunk, processed, block)
      return if points_chunk.nil? || points_chunk.empty?

      colors_arg = colors_chunk
      colors_arg = nil if colors_arg.is_a?(Array) && colors_arg.empty?
      intensities_arg = intensities_chunk
      intensities_arg = nil if intensities_arg.is_a?(Array) && intensities_arg.empty?

      block&.call(points_chunk, colors_arg, intensities_arg, processed)
    end

    def configuration
      defined?(PointCloudImporter::Config) ? PointCloudImporter::Config : nil
    end

    def configuration_value(key, fallback)
      config = configuration
      return fallback unless config

      case key
      when :yield_interval
        config.sanitize_yield_interval(config.yield_interval)
      when :binary_buffer_size
        config.sanitize_binary_buffer_size(config.binary_buffer_size)
      when :binary_vertex_batch_size
        config.sanitize_binary_vertex_batch_size(config.binary_vertex_batch_size)
      when :chunk_size
        config.sanitize_chunk_size(config.chunk_size)
      else
        fallback
      end
    rescue StandardError
      fallback
    end

    def sanitize_positive_integer(value, default)
      candidate =
        case value
        when nil
          nil
        when Integer
          value
        when Numeric
          value.to_i
        else
          Integer(value)
        end
      candidate = nil if candidate.nil? || candidate < 1
      candidate || [default, 1].max
    rescue ArgumentError, TypeError
      [default, 1].max
    end

    def interpret_vertex(values, property_index_by_name, color_indices, intensity_index, base_offset: nil)
      base = base_offset || 0

      x = values[base + property_index_by_name['x']].to_f
      y = values[base + property_index_by_name['y']].to_f
      z = values[base + property_index_by_name['z']].to_f

      point = [x, y, z]
      color = if color_indices
                r_index, g_index, b_index = color_indices
                r = values[base + r_index].to_i
                g = values[base + g_index].to_i
                b = values[base + b_index].to_i
                ((r & 0xff) << 16) | ((g & 0xff) << 8) | (b & 0xff)
              end
      intensity = intensity_index ? values[base + intensity_index].to_f : nil
      [point, color, intensity]
    end

    def bytesize(type)
      normalized_type = type.sub(/_be\z/, '')
      case normalized_type
      when 'char', 'uchar', 'int8', 'uint8' then 1
      when 'short', 'ushort', 'int16', 'uint16' then 2
      when 'int', 'uint', 'int32', 'uint32', 'float', 'float32' then 4
      when 'double', 'float64' then 8
      else
        raise UnsupportedFormat, "Неизвестный тип #{type}"
      end
    end

    def build_property_index(properties)
      properties.each_with_object({}) do |property, acc|
        acc[property[:name]] = property[:position]
      end
    end

    def format_for_type(type, endian)
      return TYPE_MAP[type] if endian == :little

      TYPE_MAP["#{type}_be"] || TYPE_MAP[type]
    end

    def build_format_string(properties, endian:, batch_size: 1)
      @format_string_cache ||= {}
      key = [properties.map { |property| property[:type] }, endian, batch_size]
      @format_string_cache[key] ||= begin
        per_vertex = properties.map do |property|
          type = property[:type]
          format = format_for_type(type, endian)
          raise UnsupportedFormat, "Неизвестный тип #{type}" unless format

          format
        end.join

        per_vertex * batch_size
      end
    end

    def color_property_indices(property_index_by_name)
      red_name = RED_PROPERTY_NAMES.find { |name| property_index_by_name.key?(name) }
      green_name = GREEN_PROPERTY_NAMES.find { |name| property_index_by_name.key?(name) }
      blue_name = BLUE_PROPERTY_NAMES.find { |name| property_index_by_name.key?(name) }

      red_index = property_index_by_name[red_name]
      green_index = property_index_by_name[green_name]
      blue_index = property_index_by_name[blue_name]

      return nil unless red_index && green_index && blue_index

      [red_index, green_index, blue_index]
    end

    def intensity_property_index(property_index_by_name)
      INTENSITY_PROPERTY_NAMES.each do |name|
        return property_index_by_name[name] if property_index_by_name.key?(name)
      end

      nil
    end

    def validate_header!(header)
      property_index_by_name = header[:property_index_by_name]
      required = %w[x y z]
      missing = required.reject { |name| property_index_by_name.key?(name) }
      return if missing.empty?

      raise UnsupportedFormat, "Отсутствуют обязательные координаты: #{missing.join(', ')}"
    end

    def update_progress(processed_vertices:, consumed_bytes: nil)
      @progress_estimator.update(
        processed_vertices: processed_vertices,
        consumed_bytes: consumed_bytes
      )
      @estimated_progress = @progress_estimator.fraction
      report_progress(consumed_bytes: consumed_bytes)
    end

    def report_progress(force: false, consumed_bytes: nil)
      check_cancelled!
      return unless @progress_callback

      now = monotonic_time
      emit = force || ((now - @last_report_time) >= PROGRESS_REPORT_INTERVAL)
      emit ||= @estimated_progress >= 1.0
      return unless emit

      @last_report_time = now
      @progress_callback.call(
        processed_vertices: @progress_estimator.processed_vertices,
        consumed_bytes: consumed_bytes,
        total_vertices: @total_vertex_count,
        total_bytes: @progress_estimator.total_bytes,
        fraction: @estimated_progress
      )
    end

    def remaining_bytes(io)
      size = io.size
      return 0 unless size

      remaining = size - io.pos
      remaining.positive? ? remaining : 0
    rescue StandardError
      0
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue NameError, Errno::EINVAL
      Process.clock_gettime(:float_second)
    rescue NameError, ArgumentError, Errno::EINVAL
      Process.clock_gettime(Process::CLOCK_REALTIME)
    rescue NameError, Errno::EINVAL
      Time.now.to_f
    end
  end
end
