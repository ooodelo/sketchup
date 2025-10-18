# encoding: utf-8
# frozen_string_literal: true

require 'minitest/autorun'
require 'tempfile'

require_relative '../point_cloud_importer/importer'

module Sketchup
end unless defined?(Sketchup)

module Sketchup
  class << self
    attr_accessor :__test_active_model unless respond_to?(:__test_active_model=)
  end

  def self.active_model
    @__test_active_model
  end unless respond_to?(:active_model)
end

module PointCloudImporter
  class ImporterThrottleTest < Minitest::Test
    class FakeView
      attr_reader :invalidate_count

      def initialize
        @invalidate_count = 0
      end

      def invalidate
        @invalidate_count += 1
      end
    end

    FakeManager = Struct.new(:view)

    def setup
      @view = FakeView.new
      @manager = FakeManager.new(@view)
      @importer = Importer.new(@manager)
      @importer.instance_variable_set(
        :@last_view_invalidation_at,
        -Importer::VIEW_INVALIDATE_INTERVAL
      )
    end

    def test_throttled_view_invalidate_skips_fast_calls
      @importer.stub(:monotonic_time, 0.0) do
        @importer.send(:throttled_view_invalidate)
      end

      assert_equal 1, @view.invalidate_count

      @importer.stub(:monotonic_time, 0.1) do
        @importer.send(:throttled_view_invalidate)
      end

      assert_equal 1, @view.invalidate_count, 'Инвалидация не должна выполняться раньше порога'

      later_time = Importer::VIEW_INVALIDATE_INTERVAL + 0.05
      @importer.stub(:monotonic_time, later_time) do
        @importer.send(:throttled_view_invalidate)
      end

      assert_equal 2, @view.invalidate_count
    end
  end
end

module PointCloudImporter
  class ImporterChunkAggregationTest < Minitest::Test
    def setup
      manager = Struct.new(:view).new(nil)
      @importer = Importer.new(manager)
    end

    def test_compute_chunk_bounds_returns_min_max_coordinates
      points = [[-2.0, 3.0, 0.5], [4.5, -1.0, 10.0], [0.0, 2.0, -5.0]]

      bounds = @importer.send(:compute_chunk_bounds, points)

      refute_nil bounds
      assert_in_delta(-2.0, bounds[:min_x], 1e-9)
      assert_in_delta(-1.0, bounds[:min_y], 1e-9)
      assert_in_delta(-5.0, bounds[:min_z], 1e-9)
      assert_in_delta(4.5, bounds[:max_x], 1e-9)
      assert_in_delta(3.0, bounds[:max_y], 1e-9)
      assert_in_delta(10.0, bounds[:max_z], 1e-9)
    end

    def test_merge_bounds_combines_extents
      existing = { min_x: -1.0, min_y: -2.0, min_z: -3.0, max_x: 1.0, max_y: 2.0, max_z: 3.0 }
      incoming = { min_x: -5.0, min_y: 0.0, min_z: -4.0, max_x: 10.0, max_y: 5.0, max_z: 8.0 }

      merged = @importer.send(:merge_bounds, existing.dup, incoming)

      assert_in_delta(-5.0, merged[:min_x], 1e-9)
      assert_in_delta(-2.0, merged[:min_y], 1e-9)
      assert_in_delta(-4.0, merged[:min_z], 1e-9)
      assert_in_delta(10.0, merged[:max_x], 1e-9)
      assert_in_delta(5.0, merged[:max_y], 1e-9)
      assert_in_delta(8.0, merged[:max_z], 1e-9)
    end

    def test_compute_intensity_range_detects_min_max
      values = [1.2, 5.0, 0.8, 7.5]

      range = @importer.send(:compute_intensity_range, values)

      refute_nil range
      assert_in_delta(0.8, range[:min], 1e-9)
      assert_in_delta(7.5, range[:max], 1e-9)
    end

    def test_merge_intensity_range_expands_existing
      existing = { min: 0.5, max: 2.0 }
      incoming = { min: 0.2, max: 3.5 }

      merged = @importer.send(:merge_intensity_range, existing.dup, incoming)

      assert_in_delta(0.2, merged[:min], 1e-9)
      assert_in_delta(3.5, merged[:max], 1e-9)
    end
  end
end

module PointCloudImporter
  class ImporterUnitScaleTest < Minitest::Test
    FakeUnitsOptions = Struct.new(:length_unit) do
      def [](key)
        return length_unit if key == 'LengthUnit'

        nil
      end
    end

    FakeOptions = Struct.new(:units_provider) do
      def [](key)
        return units_provider if key == 'UnitsOptions'

        nil
      end
    end

    FakeModel = Struct.new(:length_unit) do
      def options
        FakeOptions.new(FakeUnitsOptions.new(length_unit))
      end
    end

    def setup
      @manager = Struct.new(:view).new(nil)
      @importer = Importer.new(@manager)
      Sketchup.__test_active_model = nil if Sketchup.respond_to?(:__test_active_model=)
    end

    def teardown
      Sketchup.__test_active_model = nil if Sketchup.respond_to?(:__test_active_model=)
    end

    def test_resolve_unit_scale_uses_model_units_when_not_provided
      Sketchup.__test_active_model = FakeModel.new(2)

      scale = @importer.send(:resolve_unit_scale, nil)

      assert_in_delta(1.0 / 25.4, scale, 1e-9)
    end

    def test_resolve_unit_scale_prefers_provided_value
      scale = @importer.send(:resolve_unit_scale, 0.5)

      assert_in_delta(0.5, scale, 1e-9)
    end

    def test_resolve_unit_scale_falls_back_to_default_when_invalid
      Sketchup.__test_active_model = FakeModel.new(nil)

      scale = @importer.send(:resolve_unit_scale, 0)

      assert_in_delta(1.0, scale, 1e-9)
    end

    def test_scale_points_mutates_coordinates_in_place
      points = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]

      @importer.send(:scale_points!, points, 2.0)

      assert_equal [[2.0, 4.0, 6.0], [8.0, 10.0, 12.0]], points
    end

    def test_detect_point_cloud_unit_from_comment
      Tempfile.create(['unit_detection', '.ply']) do |file|
        file.write("ply\n")
        file.write("comment units: meters\n")
        file.write("end_header\n")
        file.write("0\n")
        file.flush

        unit = @importer.send(:detect_point_cloud_unit, file.path)
        assert_equal :meter, unit
      end
    end

    def test_detect_point_cloud_unit_returns_nil_when_unknown
      Tempfile.create(['unit_detection_unknown', '.ply']) do |file|
        file.write("ply\n")
        file.write("comment generated by scanner\n")
        file.write("end_header\n")
        file.write("0\n")
        file.flush

        unit = @importer.send(:detect_point_cloud_unit, file.path)
        assert_nil unit
      end
    end
  end
end
