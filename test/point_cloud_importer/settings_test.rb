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
  class << self
    def defaults_store
      @defaults_store ||= Hash.new { |hash, namespace| hash[namespace] = {} }
    end

    def defaults_store=(value)
      @defaults_store = value
    end
  end

  def self.reset_defaults!
    self.defaults_store = Hash.new { |hash, namespace| hash[namespace] = {} }
  end

  def self.defaults
    defaults_store.transform_values(&:dup)
  end

  def self.defaults=(value)
    store = Hash.new { |hash, namespace| hash[namespace] = {} }
    value.each do |namespace, pairs|
      store[namespace] = pairs.dup
    end
    self.defaults_store = store
  end

  def self.read_default(namespace, key)
    defaults_store[namespace][key]
  end

  def self.write_default(namespace, key, value)
    defaults_store[namespace][key] = value
  end
end

require_relative '../../point_cloud_importer/settings'
require_relative '../../point_cloud_importer/settings_buffer'
require_relative '../../point_cloud_importer/point_cloud'

module PointCloudImporter
  class SettingsTest < Minitest::Test
    def setup
      Sketchup.reset_defaults! if Sketchup.respond_to?(:reset_defaults!)
      SettingsBuffer.instance.discard!
      reset_settings_instance!
    end

    def reset_settings_instance!
      settings = Settings.instance
      settings.instance_variable_set(:@values, Settings::DEFAULTS.dup)
    end

    def test_load_normalizes_new_integer_settings
      Sketchup.defaults = {
        Settings::PREFERENCES_NAMESPACE => {
          'values' => {
            'startup_cap' => '123',
            'invalidate_interval_ms' => '450',
            'batch_vertices_limit' => '789',
            'chunk_capacity' => '321',
            'max_points_sampled' => '654',
            'octree_max_points_per_node' => '987',
            'octree_max_depth' => '11'
          }
        }
      }

      settings = Settings.instance
      settings.load!

      assert_equal 123, settings[:startup_cap]
      assert_equal 450, settings[:invalidate_interval_ms]
      assert_equal 789, settings[:batch_vertices_limit]
      assert_equal 321, settings[:chunk_capacity]
      assert_equal 654, settings[:max_points_sampled]
      assert_equal 987, settings[:octree_max_points_per_node]
      assert_equal 11, settings[:octree_max_depth]
    end

    def test_point_cloud_uses_runtime_settings_values
      settings = Settings.instance
      settings[:chunk_capacity] = 7
      settings[:batch_vertices_limit] = 9
      settings[:startup_cap] = 12
      settings[:invalidate_interval_ms] = 400
      settings[:max_points_sampled] = 15
      settings[:octree_max_points_per_node] = 20
      settings[:octree_max_depth] = 5

      cloud = PointCloud.new(name: 'test')

      assert_equal 7, cloud.points.chunk_capacity
      assert_equal 9, cloud.send(:batch_vertices_limit)
      assert_in_delta 0.4, cloud.send(:background_invalidate_interval), 1e-6
      assert_equal 15, cloud.send(:max_background_sample_size)
      assert_equal 12, cloud.send(:resolved_startup_cap)

      octree = cloud.send(:build_octree_for_points, [Geom::Point3d.new(0.0, 0.0, 0.0)])
      refute_nil octree
      assert_equal 20, octree.max_points_per_node
      assert_equal 5, octree.max_depth
    end
  end
end
