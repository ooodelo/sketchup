# encoding: utf-8
# frozen_string_literal: true

require 'thread'
require 'tmpdir'
require 'fileutils'

module PointCloudImporter
  # Simple logger that prints diagnostic messages to the Ruby console.
  module Logger
    module_function

    LOG_REPEAT_WINDOW = 0.75
    LOG_FILENAME = 'point_cloud_importer.log'

    def debug(message = nil, &block)
      text = block ? safe_call(block) : message
      return unless text

      now = monotonic_time

      synchronize do
        append_to_log(text)

        unless logging_enabled?
          @last_emit_at = now
          next
        end

        flush_repeated(now, reason: :timeout)

        if repeated_message?(text)
          @repeat_count += 1
          @last_repeat_at = now
          next
        end

        flush_repeated(now, reason: :change)
        emit_to_console(text, now)
        remember_message(text, now)
      end
    rescue StandardError
      nil
    end

    def error(message = nil, &block)
      text = block ? safe_call(block) : message
      return unless text

      now = monotonic_time

      synchronize do
        append_to_log(text)
        emit_to_console(text, now)
        remember_message(text, now)
      end
    rescue StandardError
      nil
    end

    def safe_call(block)
      block.call
    rescue StandardError => e
      "<log failed: #{e.class}: #{e.message}>"
    end
    private_class_method :safe_call

    def logging_enabled?
      if defined?(PointCloudImporter::Config)
        PointCloudImporter::Config.logging_enabled?
      else
        true
      end
    rescue StandardError
      true
    end

    def synchronize(&block)
      mutex.synchronize(&block)
    end
    private_class_method :synchronize

    def mutex
      @mutex ||= Mutex.new
    end
    private_class_method :mutex

    def emit_to_console(text, now)
      timestamp = Time.now.strftime('%H:%M:%S')
      ::Kernel.puts("[PointCloudImporter #{timestamp}] #{text}")
      @last_emit_at = now
    end
    private_class_method :emit_to_console

    def append_to_log(text)
      path = log_path
      return unless path

      line = format_log_line(text)
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'a:UTF-8') { |file| file.puts(line) }
      path
    rescue StandardError
      nil
    end
    private_class_method :append_to_log

    def format_log_line(text)
      timestamp = Time.now.strftime('%Y-%m-%d %H:%M:%S')
      "[PointCloudImporter #{timestamp}] #{text}"
    end
    private_class_method :format_log_line

    def log_path
      @log_path ||= begin
        base = if defined?(Sketchup) && Sketchup.respond_to?(:temp_dir)
                 Sketchup.temp_dir
               else
                 Dir.tmpdir
               end
        File.join(base, LOG_FILENAME)
      rescue StandardError
        File.join(Dir.tmpdir, LOG_FILENAME)
      end
    end

    def remember_message(text, now)
      @last_message = text
      @repeat_count = 0
      @last_repeat_at = now
    end
    private_class_method :remember_message

    def repeated_message?(text)
      @last_message == text
    end
    private_class_method :repeated_message?

    def flush_repeated(now, reason:)
      return unless @last_message
      return if @repeat_count.to_i.zero?

      return if reason == :timeout && recent_repeat?(now)

      summary = format(
        '%<message>s (повторено ещё %<count>d раз)',
        message: @last_message,
        count: @repeat_count
      )

      append_to_log(summary)
      emit_to_console(summary, now)
      @repeat_count = 0
      @last_message = nil
      @last_repeat_at = nil
    end
    private_class_method :flush_repeated

    def recent_repeat?(now)
      last = @last_repeat_at
      return false unless last

      (now - last) < LOG_REPEAT_WINDOW
    rescue StandardError
      false
    end
    private_class_method :recent_repeat?

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue NameError, Errno::EINVAL
      Process.clock_gettime(:float_second)
    rescue NameError, ArgumentError, Errno::EINVAL
      Process.clock_gettime(Process::CLOCK_REALTIME)
    rescue NameError, Errno::EINVAL
      Time.now.to_f
    end
    private_class_method :monotonic_time

    at_exit do
      synchronize { flush_repeated(monotonic_time, reason: :exit) }
    end
  end
end
