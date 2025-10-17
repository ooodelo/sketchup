# encoding: utf-8
# frozen_string_literal: true

require_relative 'logger'
require_relative 'threading'
require_relative 'main_thread_queue'
require_relative 'telemetry_logger'

module PointCloudImporter
  # Cycles through color modes and gradients to benchmark repaint performance.
  class ColorBench
    INTERVAL = 0.05
    PENDING_TIMEOUT = 0.5

    def initialize(manager,
                   scheduler: MainThreadScheduler.instance,
                   telemetry: TelemetryLogger.instance)
      @manager = manager
      @scheduler = scheduler
      @telemetry = telemetry
      @task = nil
      @cloud = nil
      @scenarios = []
      @current = nil
      @initial_state = nil
    end

    def run
      Threading.guard(:ui, message: 'ColorBench#run')
      cloud = @manager.active_cloud
      unless valid_cloud?(cloud)
        Logger.debug { 'Color bench: нет активного облака для измерения' }
        return
      end

      scenarios = build_scenarios(cloud)
      if scenarios.empty?
        Logger.debug { 'Color bench: подходящих сценариев не найдено' }
        return
      end

      cancel_task

      @cloud = cloud
      @scenarios = scenarios
      @current = nil
      @initial_state = snapshot_color_state(cloud)

      Logger.debug do
        format('Color bench: запускаем %<count>d сценариев для %<cloud>s',
               count: @scenarios.length,
               cloud: cloud.name)
      end

      @task = @scheduler.schedule(name: 'color-bench', priority: 250) do |context|
        tick(context)
      end
    end

    private

    def tick(context)
      return :done unless @cloud && @cloud.points

      if @current.nil?
        scenario = @scenarios.shift
        if scenario.nil?
          finalize_bench
          return :done
        end
        start_scenario(scenario)
      else
        monitor_current_scenario
      end

      context.reschedule_in = INTERVAL
      :reschedule
    rescue StandardError => e
      Logger.debug { "Color bench tick failed: #{e.message}" }
      finalize_bench
      :done
    end

    def start_scenario(scenario)
      Logger.debug do
        format('Color bench: стартуем сценарий %<name>s', name: scenario[:name])
      end

      @current = {
        scenario: scenario,
        started_at: monotonic_time,
        pending_seen: false,
        pending_deadline: monotonic_time + PENDING_TIMEOUT,
        peak_memory: memory_usage
      }

      apply_scenario(scenario)
    rescue StandardError => e
      Logger.debug { "Color bench scenario launch failed: #{e.message}" }
      @current = nil
    end

    def monitor_current_scenario
      state = @current
      return unless state

      memory = memory_usage
      if memory
        previous = state[:peak_memory]
        state[:peak_memory] = previous ? [previous, memory].max : memory
      end

      pending = @cloud.color_rebuild_pending?
      state[:pending_seen] ||= pending

      return if pending
      return if !state[:pending_seen] && monotonic_time < state[:pending_deadline]

      finalize_scenario(state)
    end

    def finalize_scenario(state)
      summary = @cloud.consume_last_color_rebuild_summary
      finished_at = monotonic_time
      duration = extract_duration(summary, state, finished_at)
      processed = extract_processed(summary)
      rate = extract_rate(summary, processed, duration)
      peak_memory = extract_peak_memory(summary, state)
      economy_enabled = summary ? summary.dig(:economy, :enabled) : false

      notes = []
      notes << 'no_rebuild' unless state[:pending_seen]

      scenario = state[:scenario]
      telemetry_entry = {
        scenario: scenario[:name],
        color_mode: scenario[:color_mode],
        color_gradient: scenario[:color_gradient],
        duration: duration,
        processed_points: processed,
        rate: rate,
        peak_memory: peak_memory,
        economy_enabled: economy_enabled,
        notes: notes
      }
      @telemetry.log_color_bench(telemetry_entry)

      Logger.debug do
        format('color_bench;%<scenario>s;%<duration>.3f;%<rate>.1f;%<peak_memory>d;%<economy>s',
               scenario: scenario[:name],
               duration: duration || 0.0,
               rate: rate || 0.0,
               peak_memory: peak_memory.to_i,
               economy: economy_enabled ? 'economy' : 'full')
      end

      @current = nil
    rescue StandardError => e
      Logger.debug { "Color bench finalize failed: #{e.message}" }
      @current = nil
    end

    def apply_scenario(scenario)
      gradient = scenario[:color_gradient]
      color_mode = scenario[:color_mode]

      if gradient && @cloud.color_gradient != gradient
        @cloud.color_gradient = gradient
      end

      if color_mode && @cloud.color_mode != color_mode
        @cloud.color_mode = color_mode
      elsif gradient
        # Повторно применяем градиент, если режим не сменился, чтобы гарантировать перекраску
        @cloud.color_gradient = gradient
      end
    end

    def finalize_bench
      restore_initial_state
      cancel_task
      Logger.debug { 'Color bench: завершено' }
    rescue StandardError => e
      Logger.debug { "Color bench cleanup failed: #{e.message}" }
    ensure
      @task = nil
      @current = nil
      @scenarios = []
      @cloud = nil
    end

    def restore_initial_state
      return unless @cloud && @initial_state

      state = @initial_state
      begin
        if state[:color_gradient] && @cloud.color_gradient != state[:color_gradient]
          @cloud.color_gradient = state[:color_gradient]
        end
        if state[:color_mode] && @cloud.color_mode != state[:color_mode]
          @cloud.color_mode = state[:color_mode]
        end
      rescue StandardError => e
        Logger.debug { "Color bench restore failed: #{e.message}" }
      ensure
        @initial_state = nil
      end
    end

    def extract_duration(summary, state, finished_at)
      return summary[:duration] if summary && summary[:duration]

      finished_at - state[:started_at]
    rescue StandardError
      nil
    end

    def extract_processed(summary)
      return summary[:processed] if summary && summary[:processed]

      points = @cloud.points
      points ? points.length : 0
    rescue StandardError
      0
    end

    def extract_rate(summary, processed, duration)
      return summary[:rate] if summary && summary[:rate]

      return 0.0 unless duration && duration.positive?

      processed.to_f / duration
    rescue StandardError
      0.0
    end

    def extract_peak_memory(summary, state)
      return summary[:peak_memory] if summary && summary[:peak_memory]

      state[:peak_memory]
    end

    def valid_cloud?(cloud)
      cloud && cloud.respond_to?(:color_mode) && cloud.respond_to?(:color_gradient)
    end

    def build_scenarios(cloud)
      scenarios = []
      scenarios << { name: 'Original', color_mode: :original, color_gradient: cloud.color_gradient }
      scenarios << { name: 'Height / Viridis', color_mode: :height, color_gradient: :viridis }
      scenarios << { name: 'Height / Magma', color_mode: :height, color_gradient: :magma }
      if cloud.has_intensity?
        scenarios << { name: 'Intensity / Viridis', color_mode: :intensity, color_gradient: :viridis }
      end
      scenarios << { name: 'RGB XYZ', color_mode: :rgb_xyz, color_gradient: nil }
      scenarios
    end

    def snapshot_color_state(cloud)
      {
        color_mode: cloud.color_mode,
        color_gradient: cloud.color_gradient
      }
    end

    def memory_usage
      stats = GC.stat
      stats[:heap_live_slots] || stats[:heap_used] || stats[:total_allocated_objects]
    rescue StandardError
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue StandardError
      Time.now.to_f
    end

    def cancel_task
      return unless @task

      @task.cancel
      @task = nil
    end
  end
end
