# encoding: utf-8
# frozen_string_literal: true

require 'singleton'
require 'thread'

require_relative 'logger'
require_relative 'threading'

module PointCloudImporter
  # Cooperative scheduler that executes short tasks on the SketchUp main
  # thread. All UI facing background work should be funneled through this
  # singleton to avoid spawning multiple timers that compete for time slices.
  class MainThreadScheduler
    include Singleton

    DEFAULT_TICK_BUDGET = 0.007
    MAX_TASKS_PER_TICK = 128
    TIMER_INTERVAL = 0.0

    TaskHandle = Struct.new(:scheduler, :id) do
      def cancel
        scheduler&.cancel(id)
      end

      def alive?
        scheduler ? scheduler.alive?(id) : false
      end
    end

    TaskContext = Struct.new(:deadline, :handle, :scheduler) do
      attr_accessor :reschedule_in

      def remaining_budget
        return Float::INFINITY unless deadline

        [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0.0].max
      rescue StandardError
        0.0
      end
    end

    def initialize
      @mutex = Mutex.new
      @tasks = []
      @task_lookup = {}
      @next_id = 1
      @timer_id = nil
      @tasks_sorted = true
      @explicitly_started = false
      @start_warning_emitted = false
    end

    def schedule(name:, priority: 0, delay: 0.0, quota: DEFAULT_TICK_BUDGET, cancel_if: nil, &block)
      raise ArgumentError, 'block required' unless block

      task = nil

      @mutex.synchronize do
        id = @next_id
        @next_id += 1
        run_at = monotonic_time + [delay.to_f, 0.0].max
        task = {
          id: id,
          name: name.to_s,
          block: block,
          priority: priority.to_i,
          run_at: run_at,
          quota: quota.to_f.positive? ? quota.to_f : DEFAULT_TICK_BUDGET,
          cancel_if: cancel_if,
          cancelled: false
        }

        @tasks << task
        @task_lookup[id] = task
        @tasks_sorted = false
        warn_if_not_started_locked
        start_timer_locked
      end

      TaskHandle.new(self, task[:id])
    end

    def ensure_started
      Threading.guard(:ui, message: 'MainThreadScheduler#ensure_started')

      @mutex.synchronize do
        @explicitly_started = true
        start_timer_locked
      end

      true
    rescue StandardError => e
      Logger.debug do
        "Не удалось гарантировать запуск планировщика: #{e.class}: #{e.message}"
      end
      false
    end

    def cancel(id)
      @mutex.synchronize do
        task = @task_lookup.delete(id)
        task[:cancelled] = true if task
      end
    end

    def alive?(id)
      @mutex.synchronize do
        task = @task_lookup[id]
        task && !task[:cancelled]
      end
    end

    def process_tick
      Threading.guard(:ui, message: 'MainThreadScheduler#process_tick')

      tick_start = monotonic_time
      tick_deadline = tick_start + DEFAULT_TICK_BUDGET
      processed = 0

      loop do
        break if processed >= MAX_TASKS_PER_TICK

        task = next_ready_task
        break unless task

        break if monotonic_time >= tick_deadline

        handle = TaskHandle.new(self, task[:id])
        quota_deadline = [tick_deadline, monotonic_time + task[:quota]].min
        context = TaskContext.new(quota_deadline, handle, self)

        begin
          result = execute_task(task, context)
        rescue StandardError => e
          Logger.debug do
            "MainThreadScheduler task '#{task[:name]}' failed: #{e.class}: #{e.message}\n" \
              "#{Array(e.backtrace).join("\n")}"
          end
          result = :done
        end

        processed += 1

        case result
        when :pending
          reschedule_task(task, context.reschedule_in || 0.0)
        when :reschedule
          reschedule_task(task, context.reschedule_in || DEFAULT_TICK_BUDGET)
        else
          finalize_task(task)
        end
      end
    ensure
      maybe_stop_timer
    end

    def enqueue(&block)
      schedule(name: 'dispatch', priority: 1000, &block)
    end

    private

    def execute_task(task, context)
      return :done unless task
      return :done if task[:cancelled]

      predicate = task[:cancel_if]
      if predicate
        begin
          return :done if predicate.call == true
        rescue StandardError => e
          Logger.debug { "MainThreadScheduler cancel predicate failed: #{e.message}" }
        end
      end

      task[:block].call(context)
    end

    def reschedule_task(task, delay)
      @mutex.synchronize do
        return if task[:cancelled]

        task[:run_at] = monotonic_time + [delay.to_f, 0.0].max
        @tasks << task
        @task_lookup[task[:id]] = task
        @tasks_sorted = false
      end
    end

    def finalize_task(task)
      @mutex.synchronize do
        task[:cancelled] = true
        @task_lookup.delete(task[:id])
      end
    end

    def next_ready_task
      @mutex.synchronize do
        cleanup_cancelled_locked
        return nil if @tasks.empty?

        sort_tasks_locked unless @tasks_sorted

        now = monotonic_time
        return nil if @tasks.empty?
        return nil if @tasks.first[:run_at] > now

        task = @tasks.shift
        task
      end
    end

    def sort_tasks_locked
      @tasks.sort_by! { |task| [task[:run_at], -task[:priority]] }
      @tasks_sorted = true
    end

    def cleanup_cancelled_locked
      return if @tasks.empty?

      @tasks.reject! do |task|
        if task[:cancelled]
          @task_lookup.delete(task[:id])
          true
        else
          false
        end
      end
    end

    def ensure_timer_locked
      start_timer_locked
    end

    def maybe_stop_timer
      return unless defined?(::UI)

      @mutex.synchronize do
        if @tasks.empty? && @timer_id
          begin
            ::UI.stop_timer(@timer_id)
          rescue StandardError => e
            Logger.debug { "Failed to stop scheduler timer: #{e.class}: #{e.message}" }
          ensure
            @timer_id = nil
          end
        elsif @tasks.empty?
          @timer_id = nil
        end
      end
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue StandardError
      Time.now.to_f
    end

    def start_timer_locked
      return unless defined?(::UI)
      return if @timer_id

      begin
        @timer_id = ::UI.start_timer(TIMER_INTERVAL, repeat: true) { process_tick }
      rescue StandardError => e
        Logger.debug { "Failed to start scheduler timer: #{e.class}: #{e.message}" }
        @timer_id = nil
      end
    end

    def warn_if_not_started_locked
      return if @explicitly_started
      return if @start_warning_emitted

      @start_warning_emitted = true
      Logger.debug do
        'MainThreadScheduler автоматически запустил таймер. Вызовите ensure_started на UI-потоке при активации.'
      end
    end
  end

  module MainThreadDispatcher
    module_function

    def enqueue(&block)
      MainThreadScheduler.instance.enqueue(&block)
    end
  end
end
