# frozen_string_literal: true

require 'thread'

require_relative 'logger'
require_relative 'progress_estimator'

module PointCloudImporter
  # Represents a background import job with shared state between threads.
  class ImportJob
    MIN_PROGRESS_INTERVAL = 0.5

    attr_reader :path, :options
    attr_accessor :thread

    def initialize(path, options = {})
      @path = path
      @options = options.dup.freeze
      @progress = 0.0
      @message = 'Подготовка...'
      @status = :pending
      @error = nil
      @result = nil
      @cancel_requested = false
      @cloud = nil
      @cloud_added = false
      @mutex = Mutex.new
      @last_progress_time = monotonic_time - MIN_PROGRESS_INTERVAL
      @processed_vertices = 0
      @progress_estimator = ProgressEstimator.new
      Logger.debug { "Создано задание импорта для #{path.inspect}" }
    end

    def start!
      @mutex.synchronize do
        @status = :running
        @last_progress_time = monotonic_time - MIN_PROGRESS_INTERVAL
      end
      Logger.debug('Задание переведено в статус running')
    end

    def cancel!
      @mutex.synchronize do
        return if finished_locked?

        @cancel_requested = true
        @status = :cancelling if @status == :running
        @message = 'Отмена...'
      end
      Logger.debug('Получен запрос на отмену импорта')
    end

    def cancel_requested?
      @mutex.synchronize { @cancel_requested }
    end

    def mark_cancelled!
      @mutex.synchronize do
        @cancel_requested = true
        @status = :cancelled
        @progress = 0.0 if @progress.nil?
        @message = 'Импорт отменен пользователем'
      end
      Logger.debug('Задание помечено как отмененное')
    end

    def fail!(error)
      @mutex.synchronize do
        @status = :failed
        @error = error
        @message = error.message.to_s
      end
      Logger.debug { "Задание завершилось ошибкой: #{error.class}: #{error.message}" }
    end

    def complete!(result)
      @mutex.synchronize do
        @status = :completed
        @result = result
        @progress = 1.0
        @message = 'Импорт завершен'
      end
      Logger.debug('Задание успешно завершено')
    end

    def progress
      @mutex.synchronize { @progress }
    end

    def message
      @mutex.synchronize { @message }
    end

    def status
      @mutex.synchronize { @status }
    end

    def error
      @mutex.synchronize { @error }
    end

    def result
      @mutex.synchronize { @result }
    end

    def cloud
      @mutex.synchronize { @cloud }
    end

    def cloud=(value)
      @mutex.synchronize do
        @cloud = value
        @cloud_added = false if value.nil?
      end
    end

    def cloud_added?
      @mutex.synchronize { @cloud_added }
    end

    def mark_cloud_added!
      @mutex.synchronize { @cloud_added = true }
    end

    def finished?
      @mutex.synchronize { finished_locked? }
    end

    def completed?
      status == :completed
    end

    def failed?
      status == :failed
    end

    def cancelled?
      status == :cancelled
    end

    def update_progress(*args, **kwargs)
      fraction = args[0]
      text = args[1]

      message = if kwargs.key?(:message)
                  kwargs.delete(:message)
                else
                  text
                end

      processed_vertices = kwargs.delete(:processed_vertices)
      total_vertices = kwargs.delete(:total_vertices)
      total_bytes = kwargs.delete(:total_bytes)
      consumed_bytes = kwargs.delete(:consumed_bytes)
      force = kwargs.delete(:force)
      fraction_override = kwargs.delete(:fraction)

      fraction ||= fraction_override
      force ||= false

      now = monotonic_time

      @mutex.synchronize do
        return if finished_locked?

        @progress_estimator.update_totals(
          total_vertices: total_vertices,
          total_bytes: total_bytes
        ) if total_vertices || total_bytes

        if processed_vertices || consumed_bytes
          @progress_estimator.update(
            processed_vertices: processed_vertices,
            consumed_bytes: consumed_bytes
          )
        end

        estimated_fraction = @progress_estimator.fraction
        clamped_fraction = estimated_fraction.nil? ? nil : clamp_fraction(estimated_fraction)

        if clamped_fraction.nil? && !fraction.nil?
          clamped_fraction = clamp_fraction(fraction)
        end

        elapsed = now - @last_progress_time

        throttled = !force && elapsed < MIN_PROGRESS_INTERVAL
        throttled &&= clamped_fraction && clamped_fraction < 1.0
        throttled &&= message.nil? || message == @message

        if throttled
          Logger.debug do
            format(
              'Пропуск обновления прогресса: прошло %.3f с (порог %.3f с)',
              elapsed,
              MIN_PROGRESS_INTERVAL
            )
          end
          @processed_vertices = [processed_vertices.to_i, @processed_vertices].max if processed_vertices
          return
        end

        @processed_vertices = [processed_vertices.to_i, @processed_vertices].max if processed_vertices
        @progress = clamped_fraction unless clamped_fraction.nil?
        @message = message if message
        @last_progress_time = now
      end
    end

    def progress_callback
      lambda do |*args, **kwargs|
        update_progress(*args, **kwargs)
      end
    end

    private

    def finished_locked?
      %i[completed failed cancelled].include?(@status)
    end

    def clamp_fraction(value)
      return nil if value.nil?

      [[value.to_f, 0.0].max, 1.0].min
    rescue StandardError
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue NameError, Errno::EINVAL
      clock_gettime_fallback
    end

    def clock_gettime_fallback
      Process.clock_gettime(:float_second)
    rescue NameError, ArgumentError, Errno::EINVAL
      Process.clock_gettime(Process::CLOCK_REALTIME)
    rescue NameError, Errno::EINVAL
      Time.now.to_f
    end
  end
end
