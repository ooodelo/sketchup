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
end

require_relative '../point_cloud_importer/octree'

module PointCloudImporter
  class OctreeFrustumTest < Minitest::Test
    StubBoundingBox = Struct.new(:min, :max)

    def test_bounding_box_corners_returns_eight_points
      min = Geom::Point3d.new(1, 2, 3)
      max = Geom::Point3d.new(4, 5, 6)
      box = StubBoundingBox.new(min, max)
      frustum = Frustum.new([])

      corners = frustum.send(:bounding_box_corners, box)

      assert_equal 8, corners.length
      corners.each do |corner|
        assert_instance_of Geom::Point3d, corner
      end

      expected = [
        [1, 2, 3],
        [4, 2, 3],
        [1, 5, 3],
        [4, 5, 3],
        [1, 2, 6],
        [4, 2, 6],
        [1, 5, 6],
        [4, 5, 6]
      ]
      assert_equal expected, corners.map { |point| [point.x, point.y, point.z] }
    end
  end
end
