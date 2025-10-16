# encoding: utf-8
# frozen_string_literal: true

require 'minitest/autorun'

require_relative '../point_cloud_importer/importer'

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
