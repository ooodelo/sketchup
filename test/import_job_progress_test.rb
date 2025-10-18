# encoding: utf-8
# frozen_string_literal: true

require 'minitest/autorun'

require_relative '../point_cloud_importer/import_job'
require_relative '../point_cloud_importer/logger'

module PointCloudImporter
  class ImportJobProgressTest < Minitest::Test
    def test_progress_updates_use_poll_for_throttling
      job = ImportJob.new('test.ply')
      estimator = job.instance_variable_get(:@progress_estimator)

      poll_calls = []
      poll_responses = [true, false, true]
      poll_stub = lambda do |message: nil, fraction: nil, force: false, now: nil, respect_message_change: true|
        poll_calls << {
          message: message,
          fraction: fraction,
          force: force,
          now: now,
          respect_message_change: respect_message_change
        }
        poll_responses.shift
      end

      times = [0.0, 0.0, ImportJob::MIN_PROGRESS_INTERVAL + 0.1].each

      job.stub(:monotonic_time, -> { times.next }) do
        estimator.stub(:poll, poll_stub) do
          assert job.update_progress(0.1, 'Первое обновление'), 'Ожидалось успешное обновление прогресса'
          assert_equal 'Первое обновление', job.message

          refute job.update_progress(0.5, 'Второе обновление'), 'Повторное обновление должно быть отфильтровано'
          assert_equal 'Первое обновление', job.message, 'Сообщение не должно меняться при троттлинге'

          assert job.update_progress(0.9, 'Третье обновление'), 'Обновление после интервала должно пройти'
          assert_equal 'Третье обновление', job.message
        end
      end

      assert_equal 3, poll_calls.length, 'Ожидалось, что poll будет вызван для каждого обновления'
      assert poll_calls.all? { |call| call[:respect_message_change] }, 'Должен учитываться факт смены сообщения'
      assert poll_calls.all? { |call| call.key?(:force) && call[:force] == false }, 'Значение force должно передаваться'
      assert_equal ['Первое обновление', 'Второе обновление', 'Третье обновление'],
                   poll_calls.map { |call| call[:message] }
    end
  end
end
