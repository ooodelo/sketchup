# encoding: utf-8
# frozen_string_literal: true

require 'thread'

require_relative 'logger'

module PointCloudImporter
  # Utilities for documenting and enforcing threading contracts.
  module Threading
    class Violation < StandardError; end

    module_function

    THREAD_ROLE_KEY = :point_cloud_importer_role
    THREAD_NAME_KEY = :name

    def register_ui_thread!(thread = Thread.current)
      @ui_thread = thread
      set_role(thread, :ui)
      set_thread_name(thread, 'ui')
    end

    def register_background_thread!(thread = Thread.current)
      set_role(thread, :bg)
      set_thread_name(thread, 'bg') unless thread[THREAD_NAME_KEY]
    end

    def guard(expected_role, message: nil)
      role = current_role
      return true if role == expected_role

      if expected_role == :bg && background_thread_without_role?
        register_background_thread!
        return true
      end

      violation_message = message || 'Threading.guard violation'
      violation_message = format(
        '%<message>s: ожидалось %<expected>s, текущая роль %<actual>s (thread=%<thread_id>x)',
        message: violation_message,
        expected: expected_role,
        actual: role || :unknown,
        thread_id: Thread.current.object_id
      )

      Logger.debug { "THREAD VIOLATION: #{violation_message}" }
      raise Violation, violation_message
    end

    def with_role(role)
      previous = current_role
      set_role(Thread.current, role)
      yield
    ensure
      set_role(Thread.current, previous)
    end

    def current_role(thread = Thread.current)
      role = thread[THREAD_ROLE_KEY]
      return role if role

      return :ui if ui_thread?(thread)
      return :bg if background_thread?(thread)

      nil
    end

    def run_background(name: 'background', dispatcher: nil, on_failure: nil, &block)
      raise ArgumentError, 'block required' unless block

      dispatcher ||= ->(*_args) {}

      Thread.new do
        Thread.current.abort_on_exception = false
        register_background_thread!
        background_name = name.to_s.empty? ? 'background' : name.to_s
        set_thread_name(Thread.current, "bg:#{background_name}")

        begin
          guard(:bg, message: name)
          block.call
        rescue Violation => violation
          Logger.debug do
            "Background task #{name} aborted: #{violation.message}"
          end
          notify_failure(dispatcher, on_failure, violation)
        rescue StandardError => error
          Logger.error do
            <<~LOG.chomp
              Background task #{name} failed: #{error.class}: #{error.message}
              #{Array(error.backtrace).join("\n")}
            LOG
          end
          notify_failure(dispatcher, on_failure, error)
        ensure
          Thread.current[THREAD_ROLE_KEY] = nil
        end
      end
    end

    def ui_thread?(thread = Thread.current)
      ui_thread = @ui_thread
      return true if ui_thread && thread == ui_thread

      return true if thread == Thread.main

      false
    end

    def background_thread?(thread = Thread.current)
      return false if ui_thread?(thread)

      thread != Thread.main
    end

    def background_thread_without_role?
      !ui_thread?(Thread.current) && Thread.current[THREAD_ROLE_KEY].nil?
    end

    def set_role(thread, role)
      thread[THREAD_ROLE_KEY] = role
    end
    private_class_method :set_role

    def notify_failure(dispatcher, on_failure, error)
      begin
        on_failure&.call(error)
      rescue StandardError => callback_error
        Logger.error do
          "Failure callback crashed: #{callback_error.class}: #{callback_error.message}"
        end
      end

      begin
        dispatcher.call(:failed, error) if dispatcher
      rescue StandardError => dispatch_error
        Logger.error do
          "Failure dispatcher crashed: #{dispatch_error.class}: #{dispatch_error.message}"
        end
      end
    end
    private_class_method :notify_failure

    def set_thread_name(thread, name)
      thread[THREAD_NAME_KEY] = name
    rescue StandardError
      nil
    end
    private_class_method :set_thread_name
  end
end

