# frozen_string_literal: true

require 'stringio'

module PointCloudImporter
    # Parser for PLY point cloud files (ASCII, binary little endian or binary big endian).
  class PlyParser
    UnsupportedFormat = Class.new(StandardError)

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

    attr_reader :path, :total_vertex_count, :import_step

    def initialize(path, import_step: 1, progress_callback: nil)
      @path = path
      @import_step = normalize_import_step(import_step)
      @progress_callback = progress_callback
      @total_vertex_count = 0
    end

    def parse
      File.open(path, 'rb') do |io|
        header = parse_header(io)
        @total_vertex_count = header[:vertex_count] ? header[:vertex_count].to_i : 0

        validate_header!(header)

        case header[:format]
        when :ascii
          parse_ascii(io, header)
        when :binary_little
          parse_binary(io, header)
        when :binary_big
          parse_binary_big(io, header)
        else
          raise UnsupportedFormat, 'Только ASCII, binary_little_endian и binary_big_endian PLY поддерживаются.'
        end
      end
    end

    private

    def parse_header(io)
      header = { properties: [], metadata: {} }
      format_line = io.gets&.strip
      raise UnsupportedFormat, 'Не PLY файл' unless format_line == 'ply'

      until (line = io.gets&.strip).nil?
        case line
        when /^format\s+ascii/
          header[:format] = :ascii
        when /^format\s+binary_little_endian/
          header[:format] = :binary_little
        when /^format\s+binary_big_endian/
          header[:format] = :binary_big
        when /^comment\s+(.*)$/
          (header[:metadata][:comments] ||= []) << Regexp.last_match(1)
        when /^element\s+vertex\s+(\d+)/
          header[:vertex_count] = Regexp.last_match(1).to_i
          header[:properties] = []
        when /^property\s+([\w\d_]+)\s+([\w\d_]+)/
          property_type = Regexp.last_match(1)
          property_name = Regexp.last_match(2)
          position = header[:properties].length
          header[:properties] << { type: property_type, name: property_name, position: position }
        when 'end_header'
          break
        end
      end
      header[:property_index_by_name] = build_property_index(header[:properties])
      header
    end

    def parse_ascii(io, header)
      points = []
      colors = []
      vertex_count = header[:vertex_count] ? header[:vertex_count].to_i : 0
      property_index_by_name = header[:property_index_by_name]
      import_step = @import_step
      color_indices = color_property_indices(property_index_by_name)

      vertex_count.times do |index|
        report(index, vertex_count)
        line = io.gets
        break unless line

        next unless (index % import_step).zero?

        values = line.split
        point, color = interpret_vertex(values, property_index_by_name, color_indices)
        points << point
        colors << color if color
      end

      colors = nil if colors.empty?
      [points, colors, header[:metadata]]
    end

    def parse_binary(io, header)
      points = []
      colors = []
      vertex_count = header[:vertex_count] ? header[:vertex_count].to_i : 0
      properties = header[:properties]
      property_index_by_name = header[:property_index_by_name]
      import_step = @import_step
      color_indices = color_property_indices(property_index_by_name)

      stride = properties.sum { |property| bytesize(property[:type]) }
      vertex_buffer = ''.b

      vertex_count.times do |index|
        report(index, vertex_count)
        data = io.read(stride)
        break unless data && data.length == stride

        next unless (index % import_step).zero?

        vertex_buffer.replace(data)
        values = unpack_binary(vertex_buffer, properties, endian: :little)
        point, color = interpret_vertex(values, property_index_by_name, color_indices)
        points << point
        colors << color if color
      end

      colors = nil if colors.empty?
      [points, colors, header[:metadata]]
    end

    def parse_binary_big(io, header)
      points = []
      colors = []
      vertex_count = header[:vertex_count] ? header[:vertex_count].to_i : 0
      properties = header[:properties]
      property_index_by_name = header[:property_index_by_name]
      import_step = @import_step
      color_indices = color_property_indices(property_index_by_name)

      stride = properties.sum { |property| bytesize(property[:type]) }
      vertex_buffer = ''.b

      vertex_count.times do |index|
        report(index, vertex_count)
        data = io.read(stride)
        break unless data && data.length == stride

        next unless (index % import_step).zero?

        vertex_buffer.replace(data)
        values = unpack_binary(vertex_buffer, properties, endian: :big)
        point, color = interpret_vertex(values, property_index_by_name, color_indices)
        points << point
        colors << color if color
      end

      colors = nil if colors.empty?
      [points, colors, header[:metadata]]
    end

    def normalize_import_step(value)
      step = value.to_i
      step = 1 if step < 1
      step
    rescue StandardError
      1
    end

    def interpret_vertex(values, property_index_by_name, color_indices)
      x = values[property_index_by_name['x']].to_f
      y = values[property_index_by_name['y']].to_f
      z = values[property_index_by_name['z']].to_f

      point = Geom::Point3d.new(x, y, z)
      color = if color_indices
                r_index, g_index, b_index = color_indices
                Sketchup::Color.new(values[r_index].to_i, values[g_index].to_i, values[b_index].to_i)
              end
      [point, color]
    end

    def unpack_binary(buffer, properties, endian: :little)
      pointer = 0
      values = []
      properties.each do |property|
        type = property[:type]
        format = format_for_type(type, endian)
        raise UnsupportedFormat, "Неизвестный тип #{type}" unless format

        size = bytesize(type)
        slice = buffer.byteslice(pointer, size)
        pointer += size
        values << slice.unpack1(format)
      end
      values
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

    def validate_header!(header)
      property_index_by_name = header[:property_index_by_name]
      required = %w[x y z]
      missing = required.reject { |name| property_index_by_name.key?(name) }
      return if missing.empty?

      raise UnsupportedFormat, "Отсутствуют обязательные координаты: #{missing.join(', ')}"
    end

    def report(current, total)
      return unless @progress_callback
      return if total <= 0

      step = [total / 100, 1].max
      return unless (current % step).zero? || current == total - 1

      fraction = (current.to_f / [total - 1, 1].max)
      @progress_callback.call(fraction, "#{current + 1}/#{total}")
    end
  end
end
