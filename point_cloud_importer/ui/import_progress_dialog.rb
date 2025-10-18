# encoding: utf-8
# frozen_string_literal: true

require 'json'

require_relative '../logger'

module PointCloudImporter
  module UI
    # HtmlDialog wrapper that displays progress for an import job.
    class ImportProgressDialog
      TEMPLATE = File.expand_path('import_progress_dialog.html', __dir__)

      def initialize(job, on_cancel: nil, on_close: nil, on_show_log: nil)
        @job = job
        @cancel_callback = on_cancel
        @close_callback = on_close
        @show_log_callback = on_show_log
        @dialog = ::UI::HtmlDialog.new(
          dialog_title: 'Импорт облака точек',
          preferences_key: 'PointCloudImporter::ImportProgressDialog',
          resizable: false,
          width: 380,
          height: 200,
          style: ::UI::HtmlDialog::STYLE_DIALOG
        )
        register_callbacks
        @dialog.set_file(TEMPLATE)
        @dialog.set_on_closed do
          if @job.finished?
            @close_callback&.call
          else
            @cancel_callback&.call
          end
        end
      end

      def show
        @dialog.show
      end

      def close
        @dialog.close
      rescue StandardError
        nil
      end

      def update
        payload = {
          message: @job.message,
          progress: @job.progress,
          cancellable: !@job.finished?,
          status: @job.status,
          log_path: @job.failed? ? Logger.log_path : nil,
          error_message: @job.error&.message
        }
        script = "window.pciImport && window.pciImport.update(#{JSON.generate(payload)});"
        @dialog.execute_script(script)
      rescue StandardError
        nil
      end

      private

      def register_callbacks
        @dialog.add_action_callback('pci_ready') do
          update
        end
        @dialog.add_action_callback('pci_cancel_import') do
          @cancel_callback&.call
        end
        @dialog.add_action_callback('pci_close_dialog') do
          @dialog.close
        end
        @dialog.add_action_callback('pci_show_log') do |_action_context, path|
          @show_log_callback&.call(path)
        end
      end
    end
  end
end
