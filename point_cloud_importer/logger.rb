# encoding: utf-8
# frozen_string_literal: true

require 'thread'
require 'tmpdir'
require 'fileutils'

require_relative 'clock'

module PointCloudImporter
  # Simple logger that prints diagnostic messages to the Ruby console.
  module Logger
    module_function

    LOG_REPEAT_WINDOW = 0.75
    LOG_FILENAME = 'point_cloud_importer.log'

    def debug(message = nil, &block)
      text = block ? safe_call(block) : message
      return unless text

      now = Clock.monotonic
      thread_label = current_thread_label

      synchronize do
        enabled = logging_enabled?
        flush_repeated(now, reason: :timeout, enabled: enabled)

        if repeated_message?(text, thread_label)
          @repeat_count += 1
          @last_repeat_at = now
          next
        end

        flush_repeated(now, reason: :change, enabled: enabled)
        emit_message(text, now, thread_label, enabled)
        remember_message(text, now, thread_label)
      end
    rescue StandardError
      nil
    end

    def error(message = nil, &block)
      text = block ? safe_call(block) : message
      return unless text

      now = Clock.monotonic
      thread_label = current_thread_label

      synchronize do
        enabled = logging_enabled?
        flush_repeated(now, reason: :change, enabled: enabled)
        emit_message(text, now, thread_label, enabled)
        remember_message(text, now, thread_label)
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

    def emit_to_console(text, now, thread_label)
      timestamp = Clock.now.strftime('%H:%M:%S')
      label_segment = thread_label ? " #{thread_label}" : ''
      ::Kernel.puts("[PointCloudImporter #{timestamp}#{label_segment}] #{text}")
      @last_emit_at = now
    end
    private_class_method :emit_to_console

    def emit_message(text, now, thread_label, enabled)
      append_to_log(text, thread_label)
      emit_to_console(text, now, thread_label) if enabled
    end
    private_class_method :emit_message

    def append_to_log(text, thread_label)
      path = log_path
      return unless path

      line = format_log_line(text, thread_label)
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'a:UTF-8') { |file| file.puts(line) }
      path
    rescue StandardError
      nil
    end
    private_class_method :append_to_log

    def format_log_line(text, thread_label)
      timestamp = Clock.now.strftime('%Y-%m-%d %H:%M:%S')
      label_segment = thread_label ? " #{thread_label}" : ''
      "[PointCloudImporter #{timestamp}#{label_segment}] #{text}"
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

    def remember_message(text, now, thread_label)
      @last_message = text
      @repeat_count = 0
      @last_repeat_at = now
      @repeat_start_at = now
      @last_thread_label = thread_label
    end
    private_class_method :remember_message

    def repeated_message?(text, thread_label)
      @last_message == text && @last_thread_label == thread_label
    end
    private_class_method :repeated_message?

    def flush_repeated(now, reason:, enabled: true)
      return unless @last_message
      return if @repeat_count.to_i.zero?

      return if reason == :timeout && recent_repeat?(now)

      elapsed = [now - (@repeat_start_at || now), 0.0].max
      elapsed = 0.001 if elapsed.zero?
      rate = @repeat_count / elapsed

      summary = format(
        '%<message>s (повторено ещё %<count>d раз за %<elapsed>.2f с, %<rate>.2f/с)',
        message: @last_message,
        count: @repeat_count,
        elapsed: elapsed,
        rate: rate
      )

      append_to_log(summary, @last_thread_label)
      emit_to_console(summary, now, @last_thread_label) if enabled
      @repeat_count = 0
      @last_message = nil
      @last_repeat_at = nil
      @repeat_start_at = nil
      @last_thread_label = nil
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

    def current_thread_label
      thread_label_for(Thread.current)
    end
    private_class_method :current_thread_label

    def thread_label_for(thread)
      name = safe_thread_attribute(thread, :name)
      return name if name

      role = safe_thread_attribute(thread, :point_cloud_importer_role)
      return role if role

      format('thread:%#x', thread.object_id)
    rescue StandardError
      'thread:unknown'
    end
    private_class_method :thread_label_for

    def safe_thread_attribute(thread, key)
      value = thread[key]
      return nil if value.nil?

      string = value.to_s
      string.empty? ? nil : string
    rescue StandardError
      nil
    end
    private_class_method :safe_thread_attribute

    at_exit do
      synchronize do
        enabled = begin
          logging_enabled?
        rescue StandardError
          true
        end
        flush_repeated(Clock.monotonic, reason: :exit, enabled: enabled)
      end
    end
  end
end
