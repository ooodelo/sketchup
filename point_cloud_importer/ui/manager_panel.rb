# frozen_string_literal: true

require 'json'

require_relative '../settings'
require_relative 'manager_dialog'

module PointCloudImporter
  module UI
    # Dockable HtmlDialog with compact controls for clouds and quick tools.
    class ManagerPanel
      TEMPLATE = File.expand_path('manager_panel.html', __dir__)

      def initialize(manager, commands)
        @manager = manager
        @commands = commands
        settings = Settings.instance
        width = settings[:panel_width] || 340
        height = settings[:panel_height] || 440
        @dialog = ::UI::HtmlDialog.new(
          dialog_title: 'Point Cloud Manager',
          preferences_key: 'PointCloudImporter::ManagerPanel',
          style: ::UI::HtmlDialog::STYLE_DIALOG,
          resizable: true,
          scrollable: false,
          width: width,
          height: height
        )
        @dialog.set_dockable(true)
        @dialog.set_size(width, height)
        register_callbacks
        html = File.read(TEMPLATE, encoding: 'UTF-8')
        @dialog.set_html(html)
        @dialog.set_on_closed do
          size = @dialog.get_size
          width = if size.respond_to?(:width)
                    size.width
                  elsif size.respond_to?(:[]) && size.length >= 2
                    size[0]
                  end
          height = if size.respond_to?(:height)
                     size.height
                   elsif size.respond_to?(:[]) && size.length >= 2
                     size[1]
                   end
          settings[:panel_width] = width if width
          settings[:panel_height] = height if height
          settings.save!([:panel_width, :panel_height])
        end
      end

      def show
        refresh_panel
        @dialog.show
      end

      def visible?
        @dialog.visible?
      end

      def refresh!
        refresh_panel
      end

      private

      def register_callbacks
        @dialog.add_action_callback('pci_panel_ready') { refresh_panel }
        @dialog.add_action_callback('pci_panel_toggle_visibility') do |_, index|
          with_cloud(index) do |cloud|
            cloud.visible = !cloud.visible?
            cloud.sync_inference_visibility!
            @manager.view&.invalidate
            refresh_panel
          end
        end
        @dialog.add_action_callback('pci_panel_activate') do |_, index|
          with_cloud(index) do |cloud|
            @manager.active_cloud = cloud
            @manager.view&.invalidate
            refresh_panel
          end
        end
        @dialog.add_action_callback('pci_panel_open_manager') do |_, index|
          with_cloud(index) do |cloud|
            @manager.active_cloud = cloud unless cloud == @manager.active_cloud
          end
          ManagerDialog.new(@manager).show
        end
        @dialog.add_action_callback('pci_panel_remove') do |_, index|
          with_cloud(index) do |cloud|
            @manager.remove_cloud(cloud)
            refresh_panel
          end
        end
        @dialog.add_action_callback('pci_panel_import') { invoke_command(:import) }
        @dialog.add_action_callback('pci_panel_measure') { invoke_command(:measurement_tool) }
        @dialog.add_action_callback('pci_panel_export') { @manager.export_active_cloud }
        @dialog.add_action_callback('pci_panel_settings') { ManagerDialog.new(@manager).show }
      end

      def with_cloud(index)
        clouds = @manager.clouds
        cloud_index = index.to_i
        return unless cloud_index.between?(0, clouds.length - 1)

        cloud = clouds[cloud_index]
        yield(cloud) if cloud
      rescue StandardError => e
        warn("[PointCloudImporter] panel cloud error: #{e.message}")
      end

      def invoke_command(key)
        command = @commands.command(key)
        if command.respond_to?(:invoke)
          command.invoke
        elsif command
          command.call if command.respond_to?(:call)
        end
      end

      def refresh_panel
        payload = {
          clouds: serialized_clouds,
          has_active_cloud: !@manager.active_cloud.nil?
        }
        @dialog.execute_script("window.pciPanel && window.pciPanel.update(#{JSON.generate(payload)});")
      end

      def serialized_clouds
        clouds = @manager.clouds
        active = @manager.active_cloud
        clouds.each_with_index.map do |cloud, index|
          {
            index: index,
            name: cloud.name,
            points: cloud.points.length,
            visible: cloud.visible?,
            active: cloud == active,
            inference: cloud.inference_enabled?
          }
        end
      end
    end
  end
end
