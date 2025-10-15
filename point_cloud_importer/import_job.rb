# frozen_string_literal: true

require 'thread'

require_relative 'ply_parser'

module PointCloudImporter
  # Background import worker responsible for parsing point cloud files.
  class ImportJob
    Cancelled = Class.new(StandardError)

    attr_reader :path, :options

    def initialize(path, options = {})
      @path = path
      @options = options
      @progress = 0.0
      @message = ''
      @status = :pending
      @cancelled = false
      @mutex = Mutex.new
      @thread = nil
      @result = nil
      @error = nil
      @started_at = nil
      @finished_at = nil
      @duration = nil
      @last_progress_update = Time.at(0)
    end

    def start
      @mutex.synchronize do
        return self if @thread

        @status = :running
        @started_at = Time.now
        @thread = Thread.new { perform }
      end
      self
    end

    def cancel!
      @cancelled = true
    end

    def cancelled?
      @cancelled
    end

    def status
      @mutex.synchronize { @status }
    end

    def progress
      @mutex.synchronize { @progress }
    end

    def message
      @mutex.synchronize { @message }
    end

    def result
      @mutex.synchronize { @result }
    end

    def error
      @mutex.synchronize { @error }
    end

    def duration
      @mutex.synchronize { @duration }
    end

    def wait
      thread = @mutex.synchronize { @thread }
      thread&.join
    end

    private

    def perform
      parser = PlyParser.new(path, decimation: options[:decimation], progress_callback: method(:handle_progress))
      points, colors, metadata = parser.parse
      raise Cancelled if cancelled?

      finalize_success(points, colors, metadata)
    rescue Cancelled
      finalize_cancelled
    rescue StandardError => e
      finalize_error(e)
    end

    def finalize_success(points, colors, metadata)
      finish(:success) do
        @result = { points: points, colors: colors, metadata: metadata }
        @duration = Time.now - (@started_at || Time.now)
        @progress = 1.0
        @message = 'Готово'
      end
    end

    def finalize_cancelled
      finish(:cancelled) do
        @message = 'Отменено'
      end
    end

    def finalize_error(error)
      finish(:failed) do
        @error = error
        @message = error.message.to_s
      end
    end

    def finish(status)
      @mutex.synchronize do
        yield if block_given?
        @status = status
        @finished_at = Time.now
      end
    end

    def handle_progress(percent, message)
      raise Cancelled if cancelled?

      now = Time.now
      normalized = [[percent.to_f, 0.0].max, 1.0].min
      text = message.to_s

      @mutex.synchronize do
        return if normalized < 1.0 && (now - @last_progress_update) < 0.1

        @progress = normalized
        @message = text
        @last_progress_update = now
      end
    end
  end
end
