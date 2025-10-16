# encoding: utf-8
# frozen_string_literal: true

module PointCloudImporter
  # Simple logger that prints diagnostic messages to the Ruby console.
  module Logger
    module_function

    def debug(message = nil, &block)
      return unless logging_enabled?

      text = if block
               safe_call(block)
             else
               message
             end
      return unless text

      timestamp = Time.now.strftime('%H:%M:%S')
      ::Kernel.puts("[PointCloudImporter #{timestamp}] #{text}")
    rescue StandardError
      nil
    end

    def safe_call(block)
      block.call
    rescue StandardError => e
      "<log failed: #{e.class}: #{e.message}>"
    end
    private_class_method :safe_call

    def logging_enabled?
      if defined?(PointCloudImporter::Config)
        PointCloudImporter::Config.logging_enabled?
      else
        true
      end
    rescue StandardError
      true
    end
  end
end
