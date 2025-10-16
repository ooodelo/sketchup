# encoding: utf-8
# frozen_string_literal: true

require 'json'

module PointCloudImporter
  module UI
    # HtmlDialog wrapper that displays progress for an import job.
    class ImportProgressDialog
      TEMPLATE = File.expand_path('import_progress_dialog.html', __dir__)

      def initialize(job, &cancel_callback)
        @job = job
        @cancel_callback = cancel_callback
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
          @cancel_callback&.call unless @job.finished?
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
          cancellable: !@job.finished?
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
      end
    end
  end
end
