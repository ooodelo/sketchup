# encoding: utf-8
# frozen_string_literal: true

require 'sketchup.rb'

require_relative 'settings'
require_relative 'manager'
require_relative 'ui/commands'
require_relative 'ui/first_import_wizard'

module PointCloudImporter
  module Config
    extend self

    DEFAULTS = {
      chunk_size: 1_000_000,
      yield_interval: 10_000,
      binary_buffer_size: 4_194_304,
      binary_vertex_batch_size: 8_192,
      invalidate_every_n_chunks: 25,
      build_octree_async: true,
      logging_enabled: true,
      metrics_enabled: false
    }.freeze

    def chunk_size
      @chunk_size ||= DEFAULTS[:chunk_size]
    end

    def chunk_size=(value)
      @chunk_size = positive_integer(value, DEFAULTS[:chunk_size])
    end

    def yield_interval
      @yield_interval ||= DEFAULTS[:yield_interval]
    end

    def yield_interval=(value)
      @yield_interval = positive_integer(value, DEFAULTS[:yield_interval])
    end

    def binary_buffer_size
      @binary_buffer_size ||= DEFAULTS[:binary_buffer_size]
    end

    def binary_buffer_size=(value)
      @binary_buffer_size = positive_integer(value, DEFAULTS[:binary_buffer_size])
    end

    def binary_vertex_batch_size
      @binary_vertex_batch_size ||= DEFAULTS[:binary_vertex_batch_size]
    end

    def binary_vertex_batch_size=(value)
      @binary_vertex_batch_size = positive_integer(value, DEFAULTS[:binary_vertex_batch_size])
    end

    def invalidate_every_n_chunks
      @invalidate_every_n_chunks ||= DEFAULTS[:invalidate_every_n_chunks]
    end

    def invalidate_every_n_chunks=(value)
      @invalidate_every_n_chunks = positive_integer(value,
                                                     DEFAULTS[:invalidate_every_n_chunks])
    end

    def build_octree_async?
      @build_octree_async = DEFAULTS[:build_octree_async] if @build_octree_async.nil?
      !!@build_octree_async
    end

    def build_octree_async=(value)
      @build_octree_async = boolean(value, DEFAULTS[:build_octree_async])
    end

    def logging_enabled?
      @logging_enabled = DEFAULTS[:logging_enabled] if @logging_enabled.nil?
      !!@logging_enabled
    end

    def logging_enabled=(value)
      @logging_enabled = boolean(value, DEFAULTS[:logging_enabled])
    end

    def metrics_enabled?
      @metrics_enabled = DEFAULTS[:metrics_enabled] if @metrics_enabled.nil?
      !!@metrics_enabled
    end

    def metrics_enabled=(value)
      @metrics_enabled = boolean(value, DEFAULTS[:metrics_enabled])
    end

    def load_from_settings(settings = Settings.instance)
      self.chunk_size = settings[:import_chunk_size]
      self.invalidate_every_n_chunks = settings[:invalidate_every_n_chunks]
      self.yield_interval = settings[:yield_interval]
      self.binary_buffer_size = settings[:binary_buffer_size]
      self.binary_vertex_batch_size = settings[:binary_vertex_batch_size]
      self.build_octree_async = settings[:build_octree_async]
      self.logging_enabled = settings[:logging_enabled]
      self.metrics_enabled = settings[:metrics_enabled]
      self
    end

    def apply_setting(key, value)
      case key.to_sym
      when :import_chunk_size
        self.chunk_size = value
      when :invalidate_every_n_chunks
        self.invalidate_every_n_chunks = value
      when :yield_interval
        self.yield_interval = value
      when :binary_buffer_size
        self.binary_buffer_size = value
      when :binary_vertex_batch_size
        self.binary_vertex_batch_size = value
      when :build_octree_async
        self.build_octree_async = value
      when :logging_enabled
        self.logging_enabled = value
      when :metrics_enabled
        self.metrics_enabled = value
      end
    end

    def sanitize_chunk_size(value)
      positive_integer(value, chunk_size)
    end

    def sanitize_yield_interval(value)
      positive_integer(value, yield_interval)
    end

    def sanitize_binary_buffer_size(value)
      positive_integer(value, binary_buffer_size)
    end

    def sanitize_binary_vertex_batch_size(value)
      positive_integer(value, binary_vertex_batch_size)
    end

    def sanitize_invalidate_every_n_chunks(value)
      positive_integer(value, invalidate_every_n_chunks)
    end

    private

    def positive_integer(value, default)
      int = normalize_integer(value)
      int = nil if int.nil? || int < 1
      int || [default, 1].max
    end

    def boolean(value, default)
      case value
      when nil
        default
      when true, false
        value
      when Numeric
        !value.to_i.zero?
      else
        str = value.to_s.strip.downcase
        return true if %w[true 1 yes on].include?(str)
        return false if %w[false 0 no off].include?(str)
        default
      end
    end

    def normalize_integer(value)
      case value
      when nil
        nil
      when Integer
        value
      when Numeric
        value.to_i
      else
        Integer(value)
      end
    rescue ArgumentError, TypeError
      nil
    end
  end

  module Extension
    extend self

    def activate
      Thread.abort_on_exception = true
      PointCloudImporter::Settings.instance.load!
      PointCloudImporter::Config.load_from_settings(PointCloudImporter::Settings.instance)
      manager = PointCloudImporter::Manager.instance
      PointCloudImporter::UI::Commands.instance(manager).register!
      PointCloudImporter::UI::FirstImportWizard.show_if_needed(manager)
    end

    activate
  end
end
