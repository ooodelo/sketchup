# frozen_string_literal: true

require 'thread'

module PointCloudImporter
  # Represents a background import job with shared state between threads.
  class ImportJob
    MIN_PROGRESS_INTERVAL = 0.1

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
      @last_progress_time = Process.clock_gettime(:float_second) - MIN_PROGRESS_INTERVAL
    end

    def start!
      @mutex.synchronize do
        @status = :running
        @last_progress_time = Process.clock_gettime(:float_second) - MIN_PROGRESS_INTERVAL
      end
    end

    def cancel!
      @mutex.synchronize do
        return if finished?

        @cancel_requested = true
        @status = :cancelling if @status == :running
        @message = 'Отмена...'
      end
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
    end

    def fail!(error)
      @mutex.synchronize do
        @status = :failed
        @error = error
        @message = error.message.to_s
      end
    end

    def complete!(result)
      @mutex.synchronize do
        @status = :completed
        @result = result
        @progress = 1.0
        @message = 'Импорт завершен'
      end
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
      %i[completed failed cancelled].include?(status)
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

    def update_progress(fraction, text)
      clamped_fraction = clamp_fraction(fraction)
      now = Process.clock_gettime(:float_second)

      @mutex.synchronize do
        return if finished?

        if clamped_fraction && clamped_fraction < 1.0 && (now - @last_progress_time) < MIN_PROGRESS_INTERVAL
          return if !text || text == @message
        end

        @progress = clamped_fraction unless clamped_fraction.nil?
        @message = text if text
        @last_progress_time = now
      end
    end

    def progress_callback
      lambda do |fraction, text|
        update_progress(fraction, text)
      end
    end

    private

    def clamp_fraction(value)
      return nil if value.nil?

      [[value.to_f, 0.0].max, 1.0].min
    rescue StandardError
      nil
    end
  end
end
