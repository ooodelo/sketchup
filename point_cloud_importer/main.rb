# encoding: utf-8
# frozen_string_literal: true

require 'singleton'

require_relative 'manager'
require_relative 'importer'
require_relative 'threading'
require_relative 'ui_status'
require_relative 'main_thread_queue'
require_relative 'ui/dialog_settings'

module PointCloudImporter
  # Coordinates long running import jobs with the interactive UI.
  #
  # When a user starts an import from a menu or toolbar command the feedback in
  # the viewport used to lag several seconds behind. Users assumed the
  # application froze because the HUD was not rendered until the first chunk of
  # points arrived. The bridge below activates a lightweight Tool before the
  # background job starts, mirrors ImportJob progress into the HUD, forwards
  # dialog updates to the currently building cloud and restores the previous UI
  # state after completion.
  class Main
    include Singleton

    DEFAULT_STATUS = 'Загрузка облака точек...'
    CLEANUP_DELAY = 2.0

    def initialize
      @manager = PointCloudImporter::Manager.instance
      @hud_tool = ImportHudTool.new(@manager)
      @attached_jobs = {}
      @cleanup_timer = nil
    end

    # Registers callbacks and activates the HUD for a newly created job.
    def handle_job_started(job)
      return unless job

      job_id = job.__id__
      return if @attached_jobs.key?(job_id)

      prepare_for_import
      @attached_jobs[job_id] = job
      attach_progress_listener(job)
      attach_state_listener(job)
      update_hud(job)
    end

    # Applies dialog settings coming from HtmlDialog payloads. The payload can
    # contain strings or nested arrays (Chrome on macOS often wraps arguments in
    # an array), so the values are normalised before being forwarded to the
    # active tool/cloud.
    def apply_dialog_settings(payload, target: nil)
      subject = target || settings_target
      return unless subject

      UI::DialogSettings.new(subject, @manager).apply(payload)
    end

    # Convenience helper used by HtmlDialog callbacks.
    def self.apply_dialog_settings(payload, target: nil)
      instance.apply_dialog_settings(payload, target: target)
    end

    # Installs the UI bridge around Importer#import.
    def self.install_import_hook!
      return if @hook_installed

      Importer.class_eval do
        alias_method :__pci_original_import, :import

        def import(path, options = {})
          main = PointCloudImporter::Main.instance
          main.prepare_for_import
          job = __pci_original_import(path, options)
          main.handle_job_started(job)
          job
        rescue StandardError => e
          main.handle_import_failure(e)
          raise
        end
      end

      @hook_installed = true
    end

    # Ensures the HUD tool is active and shows the default status.
    def prepare_for_import
      Threading.guard(:ui, message: 'Main#prepare_for_import')
      activate_hud(DEFAULT_STATUS)
    rescue StandardError
      # SketchUp specific guards should not break automated tests.
      nil
    end

    # Restores UI after fatal errors that occur before the job is attached.
    def handle_import_failure(error)
      message = if error && error.respond_to?(:message)
                  error.message.to_s
                end
      update_hud(nil)
      show_status("Ошибка импорта: #{message}") if message && !message.empty?
      schedule_cleanup(immediate: true)
    rescue StandardError
      nil
    end

    private

    def attach_progress_listener(job)
      job.on_progress do |updated|
        MainThreadDispatcher.enqueue do
          synchronize_cloud_reference(updated)
          update_hud(updated)
        end
      end
    end

    def attach_state_listener(job)
      job.on_state_change do |updated|
        MainThreadDispatcher.enqueue do
          synchronize_cloud_reference(updated)
          if updated.finished?
            finalize_job(updated)
          else
            update_hud(updated)
          end
        end
      end
    end

    def synchronize_cloud_reference(job)
      cloud = job.cloud
      return unless cloud

      @hud_tool.cloud = cloud unless @hud_tool.cloud.equal?(cloud)
    end

    def update_hud(job)
      status = extract_status(job)
      activate_hud(status) if status
    end

    def extract_status(job)
      return DEFAULT_STATUS unless job

      message = safe_string(job.message)
      progress = job.progress
      if progress && progress.respond_to?(:to_f)
        fraction = progress.to_f
        if fraction.finite? && fraction.positive?
          percent = (fraction * 100).round
          message = message.empty? ? "Импорт..." : message
          message = format('%<percent>d%% • %<message>s', percent: percent, message: message)
        end
      end

      message.empty? ? DEFAULT_STATUS : message
    rescue StandardError
      DEFAULT_STATUS
    end

    def safe_string(value)
      string = value.to_s
      string.respond_to?(:strip) ? string.strip : string
    rescue StandardError
      ''
    end

    def finalize_job(job)
      status_text = case job.status
                    when :completed
                      'Импорт завершен'
                    when :failed
                      failure_message(job)
                    when :cancelled
                      'Импорт отменен'
                    else
                      'Импорт завершен'
                    end

      activate_hud(status_text)
      @attached_jobs.delete(job.__id__)
      schedule_cleanup
    end

    def failure_message(job)
      error = job.error if job.respond_to?(:error)
      base = 'Импорт завершился ошибкой'
      return base unless error

      details = safe_string(error.message)
      details.empty? ? base : format('%<base>s: %<details>s', base: base, details: details)
    rescue StandardError
      'Импорт завершился ошибкой'
    end

    def schedule_cleanup(immediate: false)
      cancel_cleanup_timer

      if immediate || !defined?(::UI) || !::UI.respond_to?(:start_timer)
        clear_hud
        return
      end

      @cleanup_timer = ::UI.start_timer(CLEANUP_DELAY, false) { clear_hud }
    end

    def clear_hud
      cancel_cleanup_timer
      @hud_tool.reset!
      if UIStatus.respond_to?(:reset!)
        UIStatus.reset!
      else
        show_status('')
      end
      model = active_model
      model.select_tool(nil) if model && model.respond_to?(:select_tool)
    rescue StandardError
      nil
    end

    def cancel_cleanup_timer
      return unless defined?(::UI)
      return unless @cleanup_timer

      ::UI.stop_timer(@cleanup_timer) if ::UI.respond_to?(:stop_timer)
      @cleanup_timer = nil
    rescue StandardError
      @cleanup_timer = nil
    end

    def settings_target
      return @hud_tool if @hud_tool.supports_settings?

      active = @manager.respond_to?(:active_cloud) ? @manager.active_cloud : nil
      active || @hud_tool
    end

    def activate_hud(message)
      model = active_model
      return unless model && model.respond_to?(:select_tool)

      @hud_tool.status = message
      model.select_tool(@hud_tool) unless @hud_tool.active?
      invalidate_view
      show_status(message)
    end

    def show_status(message)
      UIStatus.set(message) if message && !message.empty?
    rescue StandardError
      nil
    end

    def invalidate_view
      view = @manager.respond_to?(:view) ? @manager.view : nil
      return unless view && view.respond_to?(:invalidate)

      view.invalidate
    rescue StandardError
      nil
    end

    def active_model
      return unless defined?(Sketchup) && Sketchup.respond_to?(:active_model)

      Sketchup.active_model
    rescue StandardError
      nil
    end

    # Tool responsible for rendering the HUD and forwarding settings changes to
    # the active cloud.
    class ImportHudTool
      BACKGROUND_PADDING = 8
      LINE_HEIGHT = 18
      CORNER = [16, 24].freeze

      attr_reader :cloud

      def initialize(manager)
        @manager = manager
        @status = nil
        @cloud = nil
        @active = false
      end

      def activate(view)
        @active = true
        draw(view)
      rescue StandardError
        nil
      end

      def deactivate(_view)
        @active = false
      rescue StandardError
        nil
      end

      def resume(view)
        draw(view)
      rescue StandardError
        nil
      end

      def active?
        @active
      end

      def status=(text)
        string = sanitize_status(text)
        return if string == @status

        @status = string
        invalidate_view
      end

      def status
        @status
      end

      def reset!
        @status = nil
        @cloud = nil
        @active = false
      end

      def cloud=(value)
        @cloud = value
      end

      def supports_settings?
        !!@cloud
      end

      # Render pipeline setters forwarded to the active cloud when available.
      def density=(value)
        apply_to_cloud(:density=, value)
      end

      def max_display_points=(value)
        method = :max_display_points=
        apply_to_cloud(method, value)
      end

      def point_size=(value)
        apply_to_cloud(:point_size=, value)
      end

      def color_mode=(value)
        apply_to_cloud(:color_mode=, value)
      end

      def color_gradient=(value)
        apply_to_cloud(:color_gradient=, value)
      end

      def single_color=(value)
        apply_to_cloud(:single_color=, value)
      end

      def memory_limit_bytes=(value)
        store = resolve_store
        return unless store && store.respond_to?(:memory_limit_bytes=)

        store.memory_limit_bytes = value
      rescue StandardError
        nil
      end

      def draw(view)
        return unless view
        return if @status.nil? || @status.empty?

        lines = @status.split("\n")
        origin = CORNER
        render_background(view, origin, lines.length)
        lines.each_with_index do |line, index|
          point = [origin[0], origin[1] + (index * LINE_HEIGHT)]
          view.draw_text(point, line)
        end
      rescue StandardError
        nil
      end

      private

      def apply_to_cloud(method, value)
        return unless @cloud
        return unless @cloud.respond_to?(method)

        @cloud.public_send(method, value)
        invalidate_view
      rescue StandardError
        nil
      end

      def resolve_store
        return nil unless @cloud

        if @cloud.respond_to?(:store)
          @cloud.store
        elsif @cloud.respond_to?(:storage)
          @cloud.storage
        end
      rescue StandardError
        nil
      end

      def sanitize_status(value)
        return '' if value.nil?

        string = value.is_a?(Array) ? value.first : value
        string = string.to_s
        string.strip
      rescue StandardError
        ''
      end

      def invalidate_view
        view = if @manager.respond_to?(:view)
                 @manager.view
               end
        return unless view && view.respond_to?(:invalidate)

        view.invalidate
      rescue StandardError
        nil
      end

      def render_background(view, origin, line_count)
        polygon = defined?(GL_POLYGON) ? GL_POLYGON : nil
        return unless polygon
        return unless view.respond_to?(:draw2d)

        height = (line_count * LINE_HEIGHT) + (BACKGROUND_PADDING * 2)
        width = 320
        points = [
          [origin[0] - BACKGROUND_PADDING, origin[1] - BACKGROUND_PADDING],
          [origin[0] + width, origin[1] - BACKGROUND_PADDING],
          [origin[0] + width, origin[1] + height - BACKGROUND_PADDING],
          [origin[0] - BACKGROUND_PADDING, origin[1] + height - BACKGROUND_PADDING]
        ]
        view.draw2d(polygon, points)
      rescue StandardError
        nil
      end
    end
  end

  Main.install_import_hook!
end

