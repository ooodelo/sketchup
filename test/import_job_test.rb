# encoding: utf-8
# frozen_string_literal: true

require 'minitest/autorun'

require_relative '../point_cloud_importer/import_job'
require_relative '../point_cloud_importer/logger'

module PointCloudImporter
  class ImportJobThrottleTest < Minitest::Test
    def test_update_progress_respects_min_interval
      job = ImportJob.new('test.ply')
      job.instance_variable_set(:@last_progress_time, -ImportJob::MIN_PROGRESS_INTERVAL)

      job.stub(:monotonic_time, 0.0) do
        job.update_progress(0.1, 'Первое обновление')
      end

      assert_equal 'Первое обновление', job.message
      assert_in_delta 0.0, job.instance_variable_get(:@last_progress_time), 1e-6

      job.stub(:monotonic_time, ImportJob::MIN_PROGRESS_INTERVAL * 0.5) do
        job.update_progress(0.5, 'Первое обновление')
      end

      assert_in_delta 0.0, job.instance_variable_get(:@last_progress_time), 1e-6,
                           'Последнее время прогресса не должно обновляться раньше порога'
      assert_equal 'Первое обновление', job.message

      job.stub(:monotonic_time, ImportJob::MIN_PROGRESS_INTERVAL + 0.1) do
        job.update_progress(0.9, 'Второе обновление')
      end

      assert_equal 'Второе обновление', job.message
      assert_in_delta ImportJob::MIN_PROGRESS_INTERVAL + 0.1,
                      job.instance_variable_get(:@last_progress_time),
                      1e-6
    end
  end
end
