# frozen_string_literal: true

require 'stringio'

module PointCloudImporter
  # Parser for PLY point cloud files (ASCII or binary little endian).
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
      'float64' => 'E'
    }.freeze

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

        case header[:format]
        when :ascii
          parse_ascii(io, header)
        when :binary_little
          parse_binary(io, header)
        else
          raise UnsupportedFormat, 'Только ASCII и binary_little_endian PLY поддерживаются.'
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
        when /^comment\s+(.*)$/
          (header[:metadata][:comments] ||= []) << Regexp.last_match(1)
        when /^element\s+vertex\s+(\d+)/
          header[:vertex_count] = Regexp.last_match(1).to_i
          header[:properties] = []
        when /^property\s+([\w\d_]+)\s+([\w\d_]+)/
          property_type = Regexp.last_match(1)
          property_name = Regexp.last_match(2)
          header[:properties] << { type: property_type, name: property_name }
        when 'end_header'
          break
        end
      end
      header
    end

    def parse_ascii(io, header)
      points = []
      colors = []
      vertex_count = header[:vertex_count] ? header[:vertex_count].to_i : 0
      properties = header[:properties]
      import_step = @import_step

      vertex_count.times do |index|
        report(index, vertex_count)
        line = io.gets
        break unless line

        next unless (index % import_step).zero?

        values = line.split
        point, color = interpret_vertex(values, properties)
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
      import_step = @import_step

      stride = properties.sum { |property| bytesize(property[:type]) }
      vertex_buffer = ''.b

      vertex_count.times do |index|
        report(index, vertex_count)
        data = io.read(stride)
        break unless data && data.length == stride

        next unless (index % import_step).zero?

        vertex_buffer.replace(data)
        values = unpack_binary(vertex_buffer, properties)
        point, color = interpret_vertex(values, properties)
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

    def interpret_vertex(values, properties)
      data = {}
      properties.each_with_index do |property, index|
        data[property[:name]] = values[index]
      end

      point = Geom::Point3d.new(data['x'].to_f, data['y'].to_f, data['z'].to_f)
      color = if data.key?('red') && data.key?('green') && data.key?('blue')
                Sketchup::Color.new(data['red'].to_i, data['green'].to_i, data['blue'].to_i)
              end
      [point, color]
    end

    def unpack_binary(buffer, properties)
      pointer = 0
      values = []
      properties.each do |property|
        type = property[:type]
        format = TYPE_MAP[type]
        raise UnsupportedFormat, "Неизвестный тип #{type}" unless format

        size = bytesize(type)
        slice = buffer.byteslice(pointer, size)
        pointer += size
        values << slice.unpack1(format)
      end
      values
    end

    def bytesize(type)
      case type
      when 'char', 'uchar', 'int8', 'uint8' then 1
      when 'short', 'ushort', 'int16', 'uint16' then 2
      when 'int', 'uint', 'int32', 'uint32', 'float', 'float32' then 4
      when 'double', 'float64' then 8
      else
        raise UnsupportedFormat, "Неизвестный тип #{type}"
      end
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
