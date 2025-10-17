# encoding: utf-8
# frozen_string_literal: true

require 'csv'
require 'fileutils'
require 'time'
require 'tmpdir'

require_relative 'importer'
require_relative 'logger'
require_relative 'threading'
require_relative 'settings'
require_relative 'main_thread_queue'

module PointCloudImporter
  # Runs a reference import and records telemetry for regression analysis.
  class StressTester
    CSV_HEADERS = %w[timestamp preset phase duration_sec status peak_memory_bytes fps notes].freeze
    DEFAULT_LOG_BASENAME = 'point_cloud_stress_metrics.csv'

    def initialize(manager, importer: Importer.new(manager))
      @manager = manager
      @importer = importer
      @telemetry_mutex = Mutex.new
      @phase_tracker = { current: nil, started_at: nil }
      @summary_logged = false
    end

    def run
      Threading.guard(:ui, message: 'StressTester#run')
      ensure_csv_log!
      preset = resolve_preset
      reference_path = resolve_reference_path

      unless reference_path
        Logger.debug do
          'Стресс-тест: эталонный файл не найден, запись результата пропущена'
        end
        log_missing_reference(preset)
        notify_missing_reference
        return nil
      end

      job = @importer.import(reference_path, import_preset: preset)
      track_job(job, preset)
      Logger.debug do
        "Стресс-тест импорта запущен для #{reference_path.inspect} (пресет=#{preset || :auto})"
      end
      job
    end

    private

    def track_job(job, preset)
      @phase_tracker[:current] = job.state
      @phase_tracker[:started_at] = Time.now

      job.on_state_change do |changed_job|
        handle_state_change(changed_job, preset)
      end

      job.on_progress do |_changed_job|
        # Progress hook reserved for future FPS/throughput metrics.
        true
      end
    end

    def handle_state_change(job, preset)
      now = Time.now
      previous = @phase_tracker[:current]
      started_at = @phase_tracker[:started_at]

      if previous && started_at
        record_phase(
          phase: previous,
          duration: now - started_at,
          status: job.state,
          preset: preset
        )
      end

      @phase_tracker[:current] = job.state
      @phase_tracker[:started_at] = now

      finalize_summary(job, preset) if terminal_state?(job.state)
    end

    def finalize_summary(job, preset)
      return if @summary_logged

      @summary_logged = true
      MainThreadDispatcher.enqueue do
        total_duration = job.result && job.result[:duration]
        record_phase(
          phase: 'summary',
          duration: total_duration,
          status: job.status,
          preset: preset
        )
      end
    end

    def record_phase(phase:, duration:, status:, preset:)
      timestamp = Time.now.utc.iso8601
      duration_value = duration ? format('%.3f', duration) : nil
      peak_memory = fetch_peak_memory_bytes
      fps_value = sample_fps
      notes = []

      if duration && duration >= long_phase_threshold
        notes << 'slow_phase'
        Logger.debug do
          format('Стресс-тест: стадия %<phase>s длилась %.2f с (порог %.2f с)',
                 phase: phase,
                 duration: duration,
                 threshold: long_phase_threshold)
        end
      end

      append_csv_row([
        timestamp,
        (preset || :auto).to_s,
        phase.to_s,
        duration_value,
        status.to_s,
        peak_memory,
        fps_value,
        notes.join('|')
      ])
    rescue StandardError => e
      Logger.debug do
        "Стресс-тест: не удалось записать телеметрию для стадии #{phase}: #{e.message}"
      end
    end

    def append_csv_row(row)
      path = csv_path
      @telemetry_mutex.synchronize do
        CSV.open(path, 'a', force_quotes: false) do |csv|
          csv << row
        end
      end
    rescue StandardError => e
      Logger.debug do
        "Стресс-тест: ошибка записи в CSV #{path.inspect}: #{e.message}"
      end
    end

    def ensure_csv_log!
      path = csv_path
      directory = File.dirname(path)
      FileUtils.mkdir_p(directory) unless Dir.exist?(directory)
      return if File.exist?(path) && File.size?(path)

      CSV.open(path, 'w', force_quotes: false) do |csv|
        csv << CSV_HEADERS
      end
    rescue StandardError => e
      Logger.debug do
        "Стресс-тест: не удалось подготовить CSV-файл #{path.inspect}: #{e.message}"
      end
    end

    def csv_path
      config_path = config_value(:stress_log_path)
      settings_path = Settings.instance[:stress_log_path]
      selected = config_path || settings_path
      selected = File.join(default_log_directory, DEFAULT_LOG_BASENAME) if selected.nil? || selected.empty?
      File.expand_path(selected)
    end

    def default_log_directory
      if defined?(Sketchup) && Sketchup.respond_to?(:temp_dir)
        Sketchup.temp_dir
      else
        Dir.tmpdir
      end
    rescue StandardError
      Dir.tmpdir
    end

    def resolve_reference_path
      candidates = []
      config_reference = config_value(:stress_reference_path)
      settings_reference = Settings.instance[:stress_reference_path]
      default_docs = File.expand_path(File.join('..', 'docs', 'reference_cloud.ply'), __dir__)
      fixtures = File.expand_path(File.join('..', 'test', 'fixtures', 'reference_cloud.ply'), __dir__)

      candidates << config_reference
      candidates << settings_reference
      candidates << default_docs
      candidates << fixtures

      candidates.compact.map { |path| path.to_s.strip }.reject(&:empty?).find { |path| File.exist?(path) }
    rescue StandardError
      nil
    end

    def resolve_preset
      preset = Settings.instance[:import_preset]
      normalized = normalize_preset_value(preset)
      normalized if normalized && Settings::IMPORT_PRESETS.key?(normalized)
    rescue StandardError
      nil
    end

    def terminal_state?(state)
      %i[ready failed cancelled].include?(state)
    end

    def fetch_peak_memory_bytes
      return nil unless @importer.respond_to?(:send)

      @importer.send(:capture_peak_memory_bytes)
    rescue StandardError
      nil
    end

    def sample_fps
      return nil unless defined?(Sketchup)

      view = Sketchup.active_model&.active_view
      return nil unless view

      if view.respond_to?(:average_refresh_time)
        refresh = view.average_refresh_time
        return nil unless refresh && refresh.positive?

        format('%.2f', 1.0 / refresh)
      elsif view.respond_to?(:fps)
        fps = view.fps
        fps_value = fps.respond_to?(:to_f) ? fps.to_f : nil
        fps_value ? format('%.2f', fps_value) : nil
      end
    rescue StandardError
      nil
    end

    def long_phase_threshold
      config_value(:stress_long_phase_threshold) || Settings.instance[:stress_long_phase_threshold] || 5.0
    rescue StandardError
      5.0
    end

    def config_value(key)
      return unless defined?(PointCloudImporter::Config)
      return unless PointCloudImporter::Config.respond_to?(key)

      PointCloudImporter::Config.public_send(key)
    rescue StandardError
      nil
    end

    def log_missing_reference(preset)
      append_csv_row([
        Time.now.utc.iso8601,
        (preset || :auto).to_s,
        'missing_reference',
        nil,
        'skipped',
        nil,
        nil,
        'reference_not_found'
      ])
    end

    def notify_missing_reference
      return unless defined?(::UI) && ::UI.respond_to?(:messagebox)

      ::UI.messagebox('Стресс-тест: эталонный файл не найден. Проверьте настройки stress_reference_path.')
    rescue StandardError
      nil
    end

    def normalize_preset_value(value)
      case value
      when nil
        nil
      when Symbol
        value
      else
        text = value.to_s.strip
        return nil if text.empty?

        text.downcase.to_sym
      end
    rescue StandardError
      nil
    end
  end
end

