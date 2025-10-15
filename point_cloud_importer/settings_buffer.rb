# frozen_string_literal: true

require 'singleton'
require 'thread'

module PointCloudImporter
  # Batches preference writes and flushes them on demand.
  class SettingsBuffer
    include Singleton

    def initialize
      @pending = new_pending_hash
      @mutex = Mutex.new
    end

    def write(namespace, key, value)
      key = key.to_s
      @mutex.synchronize do
        @pending[namespace][key] = value
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
        values.each do |key, value|
          Sketchup.write_default(namespace, key, value)
        end
      end
    end

    def discard!
      @mutex.synchronize do
        @pending = new_pending_hash
      end
    end

    private

    def new_pending_hash
      Hash.new { |hash, namespace| hash[namespace] = {} }
    end
  end
end
