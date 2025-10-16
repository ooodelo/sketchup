# encoding: utf-8
# frozen_string_literal: true

require 'minitest/autorun'

module Geom
  class Point3d
    attr_reader :x, :y, :z

    def initialize(x = 0.0, y = 0.0, z = 0.0)
      @x = x
      @y = y
      @z = z
    end
  end

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

require_relative '../point_cloud_importer/octree'

module PointCloudImporter
  class OctreeFrustumTest < Minitest::Test
    def test_bounding_box_corners_returns_eight_points
      min = Geom::Point3d.new(1, 2, 3)
      max = Geom::Point3d.new(4, 5, 6)
      box = Geom::BoundingBox.new
      box.add(min)
      box.add(max)
      frustum = Frustum.new([])

      corners = frustum.send(:bounding_box_corners, box)

      assert_equal 8, corners.length
      corners.each do |corner|
        assert_instance_of Geom::Point3d, corner
      end

      x_values = corners.map(&:x).uniq.sort
      y_values = corners.map(&:y).uniq.sort
      z_values = corners.map(&:z).uniq.sort

      assert_equal [min.x, max.x], x_values
      assert_equal [min.y, max.y], y_values
      assert_equal [min.z, max.z], z_values
    end
  end
end
