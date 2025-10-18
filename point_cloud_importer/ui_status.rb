# encoding: utf-8
# frozen_string_literal: true

require 'thread'

require_relative 'clock'

module PointCloudImporter
  # Throttles SketchUp status bar updates to avoid UI jank.
  module UIStatus
    module_function

    DEFAULT_MIN_INTERVAL = 1.0 / 15.0

    def set(text, min_interval: DEFAULT_MIN_INTERVAL)
      return false unless status_available?

      message = text.to_s
      interval = normalize_interval(min_interval)
      now = Clock.monotonic

      synchronize do
        if should_emit?(message, now, interval)
          emit(message)
          remember(message, now)
          true
        else
          false
        end
      end
    rescue StandardError
      false
    end

    def reset!
      synchronize do
        @last_message = nil
        @last_emitted_at = nil
      end
      true
    rescue StandardError
      false
    end

    def status_available?
      defined?(Sketchup) && Sketchup.respond_to?(:status_text=)
    end
    private_class_method :status_available?

    def should_emit?(message, now, interval)
      last_message = @last_message
      last_emitted_at = @last_emitted_at

      return true if last_message != message
      return true unless last_emitted_at

      (now - last_emitted_at) >= interval
    rescue StandardError
      true
    end
    private_class_method :should_emit?

    def emit(message)
      Sketchup.status_text = message
    rescue StandardError
      nil
    end
    private_class_method :emit

    def remember(message, now)
      @last_message = message
      @last_emitted_at = now
    end
    private_class_method :remember

    def normalize_interval(value)
      interval = value.to_f
      interval = DEFAULT_MIN_INTERVAL if interval.nan? || interval <= 0.0
      interval
    rescue StandardError
      DEFAULT_MIN_INTERVAL
    end
    private_class_method :normalize_interval

    def synchronize(&block)
      mutex.synchronize(&block)
    end
    private_class_method :synchronize

    def mutex
      @mutex ||= Mutex.new
    end
    private_class_method :mutex
  end
end
