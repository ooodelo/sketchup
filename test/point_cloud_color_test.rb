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

unless defined?(Sketchup::Color)
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
    end
  end
end

module Sketchup
  def self.read_default(*_args)
    nil
  end unless respond_to?(:read_default)

  def self.write_default(*_args)
    true
  end unless respond_to?(:write_default)
end

require_relative '../point_cloud_importer/point_cloud'

module PointCloudImporter
  class PointCloudColorTest < Minitest::Test
    def test_append_points_packs_array_colors
      cloud = PointCloud.new(name: 'array colors')
      cloud.append_points!([[0.0, 0.0, 0.0]], [[10, 20, 30]])

      refute_nil cloud.colors
      stored = cloud.colors[0]
      assert_equal pack_color(10, 20, 30), stored
      assert_equal [10, 20, 30], cloud.send(:fetch_color_components, stored)
    end

    def test_append_points_accepts_packed_colors
      cloud = PointCloud.new(name: 'packed colors')
      packed = pack_color(40, 50, 60)
      cloud.append_points!([[0.0, 0.0, 0.0]], [packed])

      stored = cloud.colors[0]
      assert_equal packed, stored
      assert_equal [40, 50, 60], cloud.send(:fetch_color_components, stored)
    end

    def test_set_points_bulk_normalizes_colors
      cloud = PointCloud.new(name: 'bulk colors')
      packed = pack_color(70, 80, 90)
      cloud.set_points_bulk!([
                               [0.0, 0.0, 0.0],
                               [1.0, 1.0, 1.0]
                             ], [[1, 2, 3], packed])

      assert_equal pack_color(1, 2, 3), cloud.colors[0]
      assert_equal packed, cloud.colors[1]
    end

    def test_apply_point_updates_packs_color_objects
      cloud = PointCloud.new(name: 'updates')
      cloud.append_points!([[0.0, 0.0, 0.0]], [[1, 2, 3]])

      update_color = Sketchup::Color.new(100, 110, 120)
      cloud.apply_point_updates!([{ index: 0, color: update_color }])

      stored = cloud.colors[0]
      assert_equal pack_color(100, 110, 120), stored
      assert_equal [100, 110, 120], cloud.send(:fetch_color_components, stored)
    end

    private

    def pack_color(r, g, b)
      ((r & 0xff) << 16) | ((g & 0xff) << 8) | (b & 0xff)
    end
  end
end
