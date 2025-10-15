# encoding: utf-8
# frozen_string_literal: true

require 'json'
require_relative '../settings'

module PointCloudImporter
  module UI
    # HtmlDialog wrapper for managing point clouds.
    class ManagerDialog
      TEMPLATE = File.expand_path('manager_dialog.html', __dir__)

      def initialize(manager)
        @manager = manager
        settings = Settings.instance
        @dialog = ::UI::HtmlDialog.new(
          dialog_title: 'Менеджер облаков точек',
          preferences_key: 'PointCloudImporter::ManagerDialog',
          scrollable: true,
          resizable: true,
          width: settings[:dialog_width],
          height: settings[:dialog_height]
        )
        register_callbacks
        html = File.read(TEMPLATE, encoding: 'UTF-8')
        @dialog.set_html(html)
        @dialog.set_on_closed { Settings.instance.save! }
      end

      def show
        refresh
        @dialog.show
      end

      private

      def register_callbacks
        @dialog.add_action_callback('pci_ready') { refresh }
        @dialog.add_action_callback('pci_toggle_visibility') do |_, cloud_index|
          cloud = @manager.clouds[cloud_index.to_i]
          next unless cloud

          cloud.visible = !cloud.visible?
          cloud.sync_inference_visibility!
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_activate') do |_, cloud_index|
          cloud = @manager.clouds[cloud_index.to_i]
          next unless cloud

          @manager.active_cloud = cloud
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_density') do |_, cloud_index, value|
          cloud = @manager.clouds[cloud_index.to_i]
          next unless cloud

          cloud.density = value.to_f
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_point_size') do |_, cloud_index, value|
          cloud = @manager.clouds[cloud_index.to_i]
          next unless cloud

          cloud.point_size = value.to_i
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_toggle_inference') do |_, cloud_index|
          cloud = @manager.clouds[cloud_index.to_i]
          next unless cloud

          model = Sketchup.active_model
          cloud.toggle_inference_guides!(model) if model
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_remove') do |_, cloud_index|
          cloud = @manager.clouds[cloud_index.to_i]
          next unless cloud

          @manager.remove_cloud(cloud)
          refresh
        end
        @dialog.add_action_callback('pci_apply_preferences') do
          Settings.instance.save!
        end
      end

      def refresh
        payload = {
          clouds: @manager.clouds.map do |cloud|
            {
              name: cloud.name,
              points: cloud.points.length,
              visible: cloud.visible?,
              active: cloud == @manager.active_cloud,
              density: cloud.density,
              point_size: cloud.point_size,
              metadata: cloud.metadata,
              inference: cloud.inference_enabled?
            }
          end
        }
        @dialog.execute_script("window.pci && window.pci.update(#{JSON.generate(payload)});")
      end
    end
  end
end
