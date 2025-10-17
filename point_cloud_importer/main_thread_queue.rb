# encoding: utf-8
# frozen_string_literal: true

require 'singleton'
require 'thread'

require_relative 'logger'
require_relative 'threading'

module PointCloudImporter
  # Dispatch queue that ensures SketchUp API interactions are executed from the
  # main thread. Background threads enqueue blocks which are executed during
  # timer ticks to keep the UI responsive.
  class MainThreadQueue
    include Singleton

    MAX_MESSAGES_PER_TICK = 100
    MAX_TICK_DURATION = 0.008

    def initialize
      @queue = Queue.new
      @timer_mutex = Mutex.new
      @timer_id = nil
      ensure_timer
    end

    def enqueue(&block)
      raise ArgumentError, 'block required' unless block

      @queue << block
      ensure_timer
    end

    def process_tick
      Threading.guard(:ui, message: 'MainThreadQueue#process_tick')
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      processed = 0

      loop do
        break if processed >= MAX_MESSAGES_PER_TICK
        break if exceeded_budget?(start)

        block = pop_nonblocking
        break unless block

        begin
          block.call
        rescue StandardError => e
          Logger.debug do
            "MainThreadQueue handler failed: #{e.class}: #{e.message}\n#{Array(e.backtrace).join("\n")}"
          end
        ensure
          processed += 1
        end
      end
    ensure
      maybe_stop_timer
    end

    private

    def exceeded_budget?(start)
      (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) >= MAX_TICK_DURATION
    rescue StandardError
      false
    end

    def pop_nonblocking
      @queue.pop(true)
    rescue ThreadError
      nil
    end

    def ensure_timer
      return unless defined?(::UI)

      @timer_mutex.synchronize do
        return if @timer_id

        begin
          @timer_id = ::UI.start_timer(0.0, repeat: true) { process_tick }
        rescue StandardError => e
          Logger.debug { "Failed to start main thread queue timer: #{e.class}: #{e.message}" }
          @timer_id = nil
        end
      end
    end

    def maybe_stop_timer
      return unless @queue.empty?

      @timer_mutex.synchronize do
        return unless @timer_id && @queue.empty?

        begin
          ::UI.stop_timer(@timer_id)
        rescue StandardError => e
          Logger.debug { "Failed to stop main thread queue timer: #{e.class}: #{e.message}" }
        ensure
          @timer_id = nil
        end
      end
    end
  end

  module MainThreadDispatcher
    module_function

    def enqueue(&block)
      MainThreadQueue.instance.enqueue(&block)
    end
  end
end
