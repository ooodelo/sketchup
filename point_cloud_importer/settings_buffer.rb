# encoding: utf-8
# frozen_string_literal: true

require 'singleton'
require 'thread'

module PointCloudImporter
  # Batches preference writes and flushes them on demand.
  class SettingsBuffer
    include Singleton

    Snapshot = Struct.new(:version, :values) do
      def [](key)
        values[key.to_sym]
      end

      def to_h
        values.dup
      end
    end

    def initialize
      @pending = new_pending_hash
      @mutex = Mutex.new
      @version = 0
    end

    def write(namespace, key, value)
      key = key.to_s
      @mutex.synchronize do
        @pending[namespace][key] = value
        bump_version!
      end
    end

    def write_setting(key, value)
      namespace = if defined?(PointCloudImporter::Settings::PREFERENCES_NAMESPACE)
                    PointCloudImporter::Settings::PREFERENCES_NAMESPACE
                  else
                    'PointCloudImporter::Preferences'
                  end
      write(namespace, key, value)
    end

    def commit!
      pending = nil
      @mutex.synchronize do
        return if @pending.empty?

        pending = @pending
        @pending = new_pending_hash
      end

      pending.each do |namespace, values|
        if defined?(PointCloudImporter::Settings) &&
           namespace == PointCloudImporter::Settings::PREFERENCES_NAMESPACE
          normalized = PointCloudImporter::Settings.normalize!(values)
          normalized.each do |key, value|
            Sketchup.write_default(namespace, key.to_s, value)
          end
        else
          values.each do |key, value|
            Sketchup.write_default(namespace, key, value)
          end
        end
      end
    end

    def discard!
      @mutex.synchronize do
        @pending = new_pending_hash
        bump_version!
      end
    end

    def snapshot(values)
      data = nil
      version = nil
      @mutex.synchronize do
        version = @version
        data = deep_dup(values)
      end
      Snapshot.new(version, data.freeze)
    end

    def current_version
      @mutex.synchronize { @version }
    end

    private

    def new_pending_hash
      Hash.new { |hash, namespace| hash[namespace] = {} }
    end

    def bump_version!
      @version += 1
    end

    def deep_dup(object)
      case object
      when Hash
        object.each_with_object({}) do |(key, value), memo|
          memo[key] = deep_dup(value)
        end.freeze
      when Array
        object.map { |entry| deep_dup(entry) }.freeze
      else
        begin
          object.dup.freeze
        rescue TypeError
          object
        end
      end
    end
  end
end
