# encoding: utf-8
# frozen_string_literal: true

require 'thread'

require_relative 'logger'
require_relative 'progress_estimator'
require_relative 'main_thread_queue'

module PointCloudImporter
  # Represents a background import job with shared state between threads.
  class ImportJob
    MIN_PROGRESS_INTERVAL = ProgressEstimator::MIN_UPDATE_INTERVAL

    class TimeoutError < StandardError; end

    class CancellationToken
      def initialize
        @cancelled = false
        @mutex = Mutex.new
      end

      def cancel!
        @mutex.synchronize { @cancelled = true }
      end

      def cancelled?
        @mutex.synchronize { @cancelled }
      end
    end

    STATES = %i[init parsing sampling build_index register ready cancelled failed].freeze

    STATE_TRANSITIONS = {
      init: %i[parsing cancelled failed],
      parsing: %i[sampling cancelled failed],
      sampling: %i[build_index cancelled failed],
      build_index: %i[register cancelled failed],
      register: %i[ready cancelled failed],
      ready: %i[cancelled failed],
      cancelled: [],
      failed: []
    }.freeze

    STATE_TIMEOUTS = {
      parsing: 120.0,
      sampling: 60.0,
      build_index: 120.0,
      register: 30.0
    }.freeze

    attr_reader :path, :options
    attr_accessor :thread
    attr_reader :state, :cancellation_token

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
      @state = :init
      @state_entered_at = monotonic_time
      @state_deadline = compute_deadline(:init)
      @progress_listeners = []
      @state_listeners = []
      @cancellation_token = CancellationToken.new
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
        @cancellation_token.cancel!
      end
      Logger.debug('Получен запрос на отмену импорта')
    end

    def cancel_requested?
      @mutex.synchronize { @cancel_requested || @cancellation_token.cancelled? }
    end

    def mark_cancelled!
      transition_to(:cancelled)
      @mutex.synchronize do
        @cancel_requested = true
        @status = :cancelled
        @progress = 0.0 if @progress.nil?
        @message = 'Импорт отменен пользователем'
      end
      Logger.debug('Задание помечено как отмененное')
    end

    def fail!(error)
      transition_to(:failed, reason: error)
      @mutex.synchronize do
        @status = :failed
        @error = error
        @message = error.message.to_s
      end
      Logger.debug { "Задание завершилось ошибкой: #{error.class}: #{error.message}" }
    end

    def complete!(result)
      transition_to(:ready)
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

    def progress_frequency
      @mutex.synchronize { @progress_estimator.emit_frequency }
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

      listeners = nil
      updated = false
      @mutex.synchronize do
        return false if finished_locked?

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

        unless @progress_estimator.ready_to_emit?(now,
                                                  message: message,
                                                  fraction: clamped_fraction,
                                                  force: force)
          Logger.debug do
            'Пропуск обновления прогресса: сработал троттлинг (15 Гц)'
          end
          @processed_vertices = [processed_vertices.to_i, @processed_vertices].max if processed_vertices
          return false
        end

        @processed_vertices = [processed_vertices.to_i, @processed_vertices].max if processed_vertices
        @progress = clamped_fraction unless clamped_fraction.nil?
        @message = message if message
        @last_progress_time = now
        @progress_estimator.mark_emitted(now, message: message)
        listeners = @progress_listeners.dup
        updated = true
      end
      notify_listeners(listeners)
      updated
    end

    def progress_callback
      lambda do |*args, **kwargs|
        update_progress(*args, **kwargs)
      end
    end

    def on_progress(&block)
      return unless block

      @mutex.synchronize do
        @progress_listeners << block
      end
    end

    def on_state_change(&block)
      return unless block

      @mutex.synchronize do
        @state_listeners << block
      end
    end

    def transition_async(state, reason: nil)
      MainThreadDispatcher.enqueue { transition_to(state, reason: reason) }
    end

    def transition_to(state, reason: nil)
      listeners = nil
      previous = nil
      deadline = nil
      timestamp = monotonic_time

      @mutex.synchronize do
        validate_state!(state)
        return @state if @state == state

        unless STATE_TRANSITIONS.fetch(@state).include?(state)
          raise ArgumentError, format('Недопустимый переход из %<from>s в %<to>s', from: @state, to: state)
        end

        previous = @state
        @state = state
        @state_entered_at = timestamp
        deadline = compute_deadline(state)
        @state_deadline = deadline
        listeners = @state_listeners.dup
      end

      Logger.debug do
        details = ["Переход задания #{path.inspect} из состояния #{previous} в #{state}"]
        details << "причина: #{reason}" if reason
        details << format('тайм-аут: %.1f c', deadline) if deadline
        details.join(', ')
      end

      notify_listeners(listeners)
      state
    rescue ArgumentError => e
      Logger.debug { "Сбой перехода состояния: #{e.message}" }
      state
    end

    def ensure_state_fresh!
      deadline = nil
      current_state = nil
      @mutex.synchronize do
        deadline = @state_deadline
        current_state = @state
      end

      return if deadline.nil?
      return if monotonic_time <= deadline

      raise TimeoutError, "Превышено время для состояния #{current_state}"
    end

    private

    def notify_listeners(listeners)
      Array(listeners).each do |listener|
        begin
          listener.call(self)
        rescue StandardError => e
          Logger.debug { "Обработчик уведомлений завершился с ошибкой: #{e.class}: #{e.message}" }
        end
      end
    end

    def compute_deadline(state)
      timeout = STATE_TIMEOUTS[state]
      return nil unless timeout && timeout.positive?

      monotonic_time + timeout
    end

    def validate_state!(state)
      return if STATES.include?(state)

      raise ArgumentError, "Неизвестное состояние #{state.inspect}"
    end

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
