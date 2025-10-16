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

      def offset(vector, distance)
        Geom::Point3d.new(
          x + (vector.x * distance),
          y + (vector.y * distance),
          z + (vector.z * distance)
        )
      end

      def -(other)
        VectorStub.new(x - other.x, y - other.y, z - other.z)
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
      def initialize(*_args); end
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

VectorStub = Struct.new(:x, :y, :z) do
  def length
    Math.sqrt((x * x) + (y * y) + (z * z))
  end

  def normalize!
    magnitude = length
    return self if magnitude.zero?

    self.x /= magnitude
    self.y /= magnitude
    self.z /= magnitude
    self
  end

  def dot(other)
    (x * other.x) + (y * other.y) + (z * other.z)
  end
end

require_relative '../point_cloud_importer/point_cloud'

module PointCloudImporter
  class RayBoundsSegmentTest < Minitest::Test
    def setup
      @cloud = PointCloud.new(name: 'ray test')
    end

    def test_parallel_axis_inside_bounds_returns_segment
      origin = Geom::Point3d.new(5.0, 5.0, 5.0)
      direction = VectorStub.new(0.0, 1.0, 1e-10)
      min_point = Geom::Point3d.new(0.0, 0.0, 0.0)
      max_point = Geom::Point3d.new(10.0, 10.0, 10.0)

      segment = @cloud.send(:ray_bounds_segment, origin, direction, min_point, max_point)

      refute_nil segment
      assert_in_delta(-5.0, segment.first, 1e-9)
      assert_in_delta(5.0, segment.last, 1e-9)
    end

    def test_parallel_axis_outside_bounds_returns_nil
      origin = Geom::Point3d.new(15.0, 5.0, 5.0)
      direction = VectorStub.new(0.0, 1.0, 0.0)
      min_point = Geom::Point3d.new(0.0, 0.0, 0.0)
      max_point = Geom::Point3d.new(10.0, 10.0, 10.0)

      segment = @cloud.send(:ray_bounds_segment, origin, direction, min_point, max_point)

      assert_nil segment
    end
  end
end
