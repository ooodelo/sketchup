# encoding: utf-8
# frozen_string_literal: true

require 'csv'
require 'fileutils'
require 'singleton'
require 'thread'
require 'time'
require 'tmpdir'

require_relative 'clock'
require_relative 'logger'
require_relative 'settings'
begin
  require_relative 'extension'
rescue LoadError
  # Allow tests to run without SketchUp runtime dependencies.
end

module PointCloudImporter
  # Persists telemetry samples to CSV files for offline analysis.
  class TelemetryLogger
    include Singleton

    COLOR_HEADERS = %w[timestamp cloud generation success processed total primary_total duration_sec primary_duration_sec rate_pts_per_sec peak_memory_bytes color_mode economy_enabled economy_step economy_visible economy_threshold].freeze
    COLOR_BENCH_HEADERS = %w[timestamp scenario color_mode color_gradient duration_sec processed_points rate_pts_per_sec peak_memory_bytes economy_enabled notes].freeze

    def initialize
      @mutex = Mutex.new
      @prepared = {}
    end

    def log_color_rebuild(summary)
      return unless metrics_enabled?
      return unless summary

      row = [
        utc_timestamp,
        summary[:cloud].to_s,
        summary[:generation].to_i,
        summary[:success] ? 1 : 0,
        summary[:processed].to_i,
        summary[:total].to_i,
        summary[:primary_total].to_i,
        format_float(summary[:duration]),
        format_float(summary[:primary_duration]),
        format_float(summary[:rate]),
        summary[:peak_memory].to_i,
        summary[:color_mode].to_s,
        summary.dig(:economy, :enabled) ? 1 : 0,
        summary.dig(:economy, :step).to_i,
        summary.dig(:economy, :visible).to_i,
        summary.dig(:economy, :threshold).to_i
      ]

      append_csv(color_metrics_path, COLOR_HEADERS, row)
    end

    def log_color_bench(entry)
      return unless metrics_enabled?
      return unless entry

      row = [
        utc_timestamp,
        entry[:scenario].to_s,
        entry[:color_mode].to_s,
        entry[:color_gradient] ? entry[:color_gradient].to_s : '',
        format_float(entry[:duration]),
        entry[:processed_points].to_i,
        format_float(entry[:rate]),
        entry[:peak_memory].to_i,
        entry[:economy_enabled] ? 1 : 0,
        Array(entry[:notes]).join('|')
      ]

      append_csv(color_bench_path, COLOR_BENCH_HEADERS, row)
    end

    private

    def utc_timestamp
      Clock.now.utc.iso8601
    rescue StandardError
      Clock.now.utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end

    def format_float(value)
      return nil if value.nil?

      format('%.3f', value.to_f)
    rescue StandardError
      nil
    end

    def append_csv(path, headers, row)
      @mutex.synchronize do
        ensure_headers(path, headers)
        CSV.open(path, 'a', force_quotes: false) do |csv|
          csv << row
        end
      end
    rescue StandardError => e
      Logger.debug { "TelemetryLogger: failed to append to #{path.inspect}: #{e.message}" }
    end

    def ensure_headers(path, headers)
      return if @prepared[path]

      directory = File.dirname(path)
      FileUtils.mkdir_p(directory) unless Dir.exist?(directory)

      unless File.exist?(path) && File.size?(path)
        CSV.open(path, 'w', force_quotes: false) do |csv|
          csv << headers
        end
      end

      @prepared[path] = true
    rescue StandardError => e
      Logger.debug { "TelemetryLogger: failed to prepare #{path.inspect}: #{e.message}" }
    end

    def color_metrics_path
      resolve_path(:color_metrics_log_path, 'color_metrics.csv')
    end

    def color_bench_path
      resolve_path(:color_bench_log_path, 'color_bench.csv')
    end

    def resolve_path(key, default_name)
      candidate = config_value(key)
      candidate = settings_value(key) if candidate.nil? || candidate.to_s.empty?
      candidate = File.join(default_log_directory, default_name) if candidate.nil? || candidate.to_s.empty?
      File.expand_path(candidate)
    rescue StandardError
      File.join(default_log_directory, default_name)
    end

    def config_value(key)
      return nil unless defined?(Config)

      method = key
      return Config.public_send(method) if Config.respond_to?(method)

      nil
    rescue StandardError
      nil
    end

    def settings_value(key)
      Settings.instance[key]
    rescue StandardError
      nil
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

    def metrics_enabled?
      return false unless defined?(Config)

      Config.metrics_enabled?
    rescue StandardError
      false
    end
  end
end
