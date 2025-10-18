# encoding: utf-8
# frozen_string_literal: true

require 'minitest/autorun'

module Geom
end unless defined?(Geom)

unless defined?(Geom::Point3d)
  module Geom
    class Point3d
      attr_reader :x, :y, :z

      def initialize(x = 0.0, y = 0.0, z = 0.0)
        @x = x
        @y = y
        @z = z
      end
    end
  end
end

unless defined?(Geom::BoundingBox)
  module Geom
    class BoundingBox
      attr_reader :min, :max

      def initialize
        reset
      end

      def add(point)
        @min = Geom::Point3d.new(
          [@min.x, point.x].min,
          [@min.y, point.y].min,
          [@min.z, point.z].min
        )
        @max = Geom::Point3d.new(
          [@max.x, point.x].max,
          [@max.y, point.y].max,
          [@max.z, point.z].max
        )
        self
      end

      private

      def reset
        @min = Geom::Point3d.new(Float::INFINITY, Float::INFINITY, Float::INFINITY)
        @max = Geom::Point3d.new(-Float::INFINITY, -Float::INFINITY, -Float::INFINITY)
      end
    end
  end
end

module Sketchup
end unless defined?(Sketchup)

module Sketchup
  class Color
    attr_reader :red, :green, :blue, :alpha

    def initialize(*args)
      if args.length == 1
        value = args.first
        case value
        when Color
          initialize(value.red, value.green, value.blue, value.alpha)
        when String
          hex = value.delete('#')
          hex = hex.rjust(6, '0')
          initialize(hex[0..1].to_i(16), hex[2..3].to_i(16), hex[4..5].to_i(16))
        else
          raise ArgumentError, 'Unsupported color initialization'
        end
      else
        @red = (args[0] || 0).to_i
        @green = (args[1] || 0).to_i
        @blue = (args[2] || 0).to_i
        @alpha = (args[3] || 255).to_i
      end
    end
  end unless const_defined?(:Color)

  def self.read_default(*_args)
    nil
  end unless respond_to?(:read_default)

  def self.write_default(*_args)
    true
  end unless respond_to?(:write_default)
end

require_relative '../point_cloud_importer/point_cloud'

module PointCloudImporter
  class PointCloudAppendPointsAggregatesTest < Minitest::Test
    def test_append_points_uses_provided_aggregates
      cloud = PointCloud.new(name: 'aggregated bounds')
      points = [[-1.0, 2.0, -3.0], [4.0, -5.0, 6.0]]
      colors = [[10, 20, 30], [100, 110, 120]]
      intensities = [0.25, 0.75]
      bounds = {
        min_x: -1.0,
        min_y: -5.0,
        min_z: -3.0,
        max_x: 4.0,
        max_y: 2.0,
        max_z: 6.0
      }
      intensity_range = { min: 0.25, max: 0.75 }

      bounds_calls = 0
      intensity_calls = 0

      cloud.stub(:update_bounds_with_chunk, ->(*) { bounds_calls += 1 }) do
        cloud.stub(:update_intensity_range!, ->(*) { intensity_calls += 1 }) do
          cloud.append_points!(points, colors, intensities,
                               bounds: bounds,
                               intensity_range: intensity_range)
        end
      end

      assert_equal 0, bounds_calls, 'Агрегированные границы должны исключать обход точек в главном потоке'
      assert_equal 0, intensity_calls, 'Агрегированные интенсивности должны исключать дополнительный проход'

      cloud.finalize_bounds!
      bbox = cloud.bounding_box
      assert_in_delta(-1.0, bbox.min.x, 1e-9)
      assert_in_delta(-5.0, bbox.min.y, 1e-9)
      assert_in_delta(-3.0, bbox.min.z, 1e-9)
      assert_in_delta(4.0, bbox.max.x, 1e-9)
      assert_in_delta(2.0, bbox.max.y, 1e-9)
      assert_in_delta(6.0, bbox.max.z, 1e-9)

      assert_in_delta(0.25, cloud.instance_variable_get(:@intensity_min), 1e-9)
      assert_in_delta(0.75, cloud.instance_variable_get(:@intensity_max), 1e-9)
    end

    def test_append_points_falls_back_without_aggregates
      cloud = PointCloud.new(name: 'fallback bounds')
      points = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
      intensities = [0.1, 0.9]

      bounds_calls = 0
      intensity_calls = 0
      original_bounds = cloud.method(:update_bounds_with_chunk)
      original_intensity = cloud.method(:update_intensity_range!)

      cloud.stub(:update_bounds_with_chunk, ->(chunk) { bounds_calls += 1; original_bounds.call(chunk) }) do
        cloud.stub(:update_intensity_range!, ->(values) { intensity_calls += 1; original_intensity.call(values) }) do
          cloud.append_points!(points, nil, intensities)
        end
      end

      assert_equal 1, bounds_calls, 'При отсутствии агрегатов должны использоваться текущие расчеты'
      assert_equal 1, intensity_calls, 'Диапазон интенсивности должен рассчитываться из значений'

      cloud.finalize_bounds!
      bbox = cloud.bounding_box
      assert_in_delta(1.0, bbox.min.x, 1e-9)
      assert_in_delta(2.0, bbox.min.y, 1e-9)
      assert_in_delta(3.0, bbox.min.z, 1e-9)
      assert_in_delta(4.0, bbox.max.x, 1e-9)
      assert_in_delta(5.0, bbox.max.y, 1e-9)
      assert_in_delta(6.0, bbox.max.z, 1e-9)

      assert_in_delta(0.1, cloud.instance_variable_get(:@intensity_min), 1e-9)
      assert_in_delta(0.9, cloud.instance_variable_get(:@intensity_max), 1e-9)
    end
  end
end
