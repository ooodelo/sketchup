# encoding: utf-8
# frozen_string_literal: true

require 'singleton'
require 'json'

require_relative 'manager_dialog'

module PointCloudImporter
  module UI
    # Guides users through the first point cloud import.
    class FirstImportWizard
      include Singleton

      TEMPLATE = File.expand_path('first_import_wizard.html', __dir__)
      PREFERENCES_NAMESPACE = 'PointCloudImporter::Tutorial'
      COMPLETED_KEY = 'wizard_completed'

      def self.show(manager)
        instance.manager = manager
        instance.show
      end

      def self.show_if_needed(manager)
        return if wizard_completed?

        show(manager)
      end

      def self.wizard_completed?
        value = Sketchup.read_default(PREFERENCES_NAMESPACE, COMPLETED_KEY)
        value == true || value.to_s == 'true'
      end

      def self.mark_completed!
        Sketchup.write_default(PREFERENCES_NAMESPACE, COMPLETED_KEY, true)
      end

      attr_writer :manager

      def initialize
        @manager = nil
        @dialog = nil
      end

      def show
        ensure_dialog
        @dialog.show
        @dialog.bring_to_front if @dialog.respond_to?(:bring_to_front)
      end

      private

      def ensure_dialog
        return if @dialog && (!@dialog.respond_to?(:visible?) || @dialog.visible?)

        @dialog = ::UI::HtmlDialog.new(
          dialog_title: 'Добро пожаловать в Point Cloud Importer',
          preferences_key: 'PointCloudImporter::FirstImportWizard',
          resizable: false,
          width: 520,
          height: 540,
          style: ::UI::HtmlDialog::STYLE_DIALOG
        )
        register_callbacks
        @dialog.set_file(TEMPLATE)
        @dialog.set_on_closed { self.class.mark_completed! }
      end

      def register_callbacks
        @dialog.add_action_callback('pci_wizard_ready') do
          payload = {
            manager_label: manager_button_label
          }
          script = "window.pciWizard && window.pciWizard.bootstrap(#{JSON.generate(payload)});"
          @dialog.execute_script(script)
        end
        @dialog.add_action_callback('pci_wizard_complete') do
          self.class.mark_completed!
          @dialog.close
        end
        @dialog.add_action_callback('pci_wizard_open_manager') do
          open_manager_dialog
        end
      end

      def manager_button_label
        'Менеджер облаков'
      end

      def open_manager_dialog
        return unless @manager

        ManagerDialog.new(@manager).show
      end
    end
  end
end
