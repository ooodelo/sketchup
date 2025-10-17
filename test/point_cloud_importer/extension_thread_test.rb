# encoding: utf-8
# frozen_string_literal: true

$LOADED_FEATURES << 'sketchup.rb' unless $LOADED_FEATURES.include?('sketchup.rb')

module Sketchup
  class << self
    unless respond_to?(:defaults_store)
      def defaults_store
        @defaults_store ||= Hash.new { |hash, namespace| hash[namespace] = {} }
      end
    end

    unless respond_to?(:defaults_store=)
      def defaults_store=(value)
        @defaults_store = value
      end
    end

    unless respond_to?(:read_default)
      def read_default(namespace, key)
        defaults_store[namespace][key]
      end
    end

    unless respond_to?(:write_default)
      def write_default(namespace, key, value)
        defaults_store[namespace][key] = value
      end
    end

    unless respond_to?(:file_loaded?)
      def file_loaded?(_path)
        false
      end
    end

    unless respond_to?(:file_loaded)
      def file_loaded(_path)
        true
      end
    end

    unless respond_to?(:register_extension)
      def register_extension(*); end
    end

    unless respond_to?(:active_model)
      def active_model
        @active_model ||= Model.new
      end
    end

    unless respond_to?(:active_model=)
      def active_model=(model)
        @active_model = model
      end
    end
  end

  unless const_defined?(:Overlay)
    class Overlay
    end
  end

  unless const_defined?(:Model)
    class Model
      attr_reader :active_view

      def initialize
        @active_view = View.new
      end

      def select_tool(_tool); end

      def model_overlays
        @model_overlays ||= ModelOverlays.new
      end
    end
  end

  unless const_defined?(:ModelOverlays)
    class ModelOverlays
      def add(_overlay); end
      def remove(_overlay); end
    end
  end

  unless const_defined?(:View)
    class View
      def invalidate; end
      def pickray(*); nil; end
    end
  end
end

module Geom
end unless defined?(Geom)

unless defined?(Geom::Point3d)
  module Geom
    class Point3d
      attr_reader :x, :y, :z

      def initialize(x = 0.0, y = 0.0, z = 0.0)
        if y.nil? && z.nil? && x.respond_to?(:to_a)
          coords = x.to_a
          x = coords[0] || 0.0
          y = coords[1] || 0.0
          z = coords[2] || 0.0
        end
        @x = x.to_f
        @y = y.to_f
        @z = z.to_f
      end

      def to_a
        [@x, @y, @z]
      end

      def distance(other)
        dx = @x - other.x
        dy = @y - other.y
        dz = @z - other.z
        Math.sqrt(dx * dx + dy * dy + dz * dz)
      end
    end
  end
end

unless defined?(Geom::Vector3d)
  module Geom
    class Vector3d
      attr_reader :x, :y, :z

      def initialize(x = 0.0, y = 0.0, z = 0.0)
        @x = x.to_f
        @y = y.to_f
        @z = z.to_f
      end

      def length
        Math.sqrt(@x * @x + @y * @y + @z * @z)
      end

      def cross(other)
        self.class.new(
          @y * other.z - @z * other.y,
          @z * other.x - @x * other.z,
          @x * other.y - @y * other.x
        )
      end

      def normalize!
        len = length
        return self if len.zero?

        @x /= len
        @y /= len
        @z /= len
        self
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
        x, y, z = point.to_a
        @min = Point3d.new([@min.x, x].min, [@min.y, y].min, [@min.z, z].min)
        @max = Point3d.new([@max.x, x].max, [@max.y, y].max, [@max.z, z].max)
        self
      end

      private

      def reset
        @min = Point3d.new(Float::INFINITY, Float::INFINITY, Float::INFINITY)
        @max = Point3d.new(-Float::INFINITY, -Float::INFINITY, -Float::INFINITY)
      end
    end
  end
end

module UI
end unless defined?(UI)

unless defined?(UI::Command)
  module UI
    class Command
      attr_accessor :tooltip, :small_icon, :large_icon

      def initialize(_name, &block)
        @callback = block
      end

      def call
        @callback&.call
      end
    end

    class Menu
      def add_submenu(_name)
        self.class.new
      end

      def add_item(_item); end

      def add_separator; end
    end

    class Toolbar
      def initialize(_name); end

      def add_item(_item); end

      def show; end
    end

    class HtmlDialog
      STYLE_DIALOG = :dialog

      def initialize(*, **); end

      def show; end

      def bring_to_front; end

      def set_file(*); end

      def set_html(*); end

      def set_on_closed(&block)
        @on_closed = block
      end

      def add_action_callback(_name)
        # No-op stub for tests
      end

      def execute_script(*); end

      def visible?
        false
      end

      def close
        @on_closed&.call
      end
    end

    def self.menu(_name)
      Menu.new
    end

    def self.start_timer(_interval, repeat: false, &block)
      block&.call
      0
    end

    def self.stop_timer(_id); end

    def self.openpanel(*); end

    def self.savepanel(*); end

    def self.messagebox(*); end
  end
end

require 'minitest/autorun'
require_relative '../../point_cloud_importer/extension'

module PointCloudImporter
  class ExtensionThreadTest < Minitest::Test
    def setup
      @original_abort_on_exception = Thread.abort_on_exception
    end

    def teardown
      Thread.abort_on_exception = @original_abort_on_exception
    end

    def test_activate_enables_global_thread_abort_on_exception
      Thread.abort_on_exception = false

      Extension.activate

      assert_equal true, Thread.abort_on_exception
    end

    def test_new_threads_propagate_exceptions_after_activation
      Thread.abort_on_exception = false

      Extension.activate

      original_report = Thread.report_on_exception if Thread.respond_to?(:report_on_exception)
      Thread.report_on_exception = false if Thread.respond_to?(:report_on_exception=)

      error = nil
      begin
        Thread.new { raise 'boom' }.join
      rescue RuntimeError => e
        error = e
      ensure
        Thread.report_on_exception = original_report if Thread.respond_to?(:report_on_exception=)
      end

      refute_nil error, 'Исключение из фонового потока должно всплывать до основного'
      assert_equal 'boom', error.message
    end
  end
end
