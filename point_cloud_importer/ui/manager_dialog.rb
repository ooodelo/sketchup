# encoding: utf-8
# frozen_string_literal: true

require 'json'
require_relative '../settings'
require_relative '../point_cloud'

module PointCloudImporter
  module UI
    # HtmlDialog wrapper for managing point clouds.
    class ManagerDialog
      TEMPLATE = File.expand_path('manager_dialog.html', __dir__)

      def initialize(manager)
        @manager = manager
        settings = Settings.instance
        @auto_apply_enabled = settings[:auto_apply_changes] != false
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
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.visible = !cloud.visible?
          cloud.sync_inference_visibility!
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_activate') do |_, cloud_index|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          @manager.active_cloud = cloud
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_preview_density') do |_, cloud_index, value|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.preview_density(value)
          @manager.view&.invalidate
        end
        @dialog.add_action_callback('pci_density') do |_, cloud_index, value|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.density = value.to_f
          @manager.view&.invalidate
          auto_apply_preferences_if_needed
          refresh
        end
        @dialog.add_action_callback('pci_preview_point_size') do |_, cloud_index, value|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.preview_point_size(value)
          @manager.view&.invalidate
        end
        @dialog.add_action_callback('pci_point_size') do |_, cloud_index, value|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.point_size = value.to_i
          @manager.view&.invalidate
          auto_apply_preferences_if_needed
          refresh
        end
        @dialog.add_action_callback('pci_color_mode') do |_, cloud_index, mode|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.color_mode = mode
          cloud.prepare_render_cache!
          @manager.view&.invalidate
          auto_apply_preferences_if_needed
          refresh
        end
        @dialog.add_action_callback('pci_color_gradient') do |_, cloud_index, gradient|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.color_gradient = gradient
          cloud.prepare_render_cache!
          @manager.view&.invalidate
          auto_apply_preferences_if_needed
          refresh
        end
        @dialog.add_action_callback('pci_single_color') do |_, cloud_index, hex|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.single_color = hex
          cloud.prepare_render_cache!
          @manager.view&.invalidate
          auto_apply_preferences_if_needed
          refresh
        end
        @dialog.add_action_callback('pci_toggle_inference') do |_, cloud_index|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          model = Sketchup.active_model
          cloud.toggle_inference_guides!(model) if model
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_toggle_octree_debug') do |_, cloud_index, value|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          cloud.octree_debug_enabled = value.to_s == 'true'
          @manager.view&.invalidate
          refresh
        end
        @dialog.add_action_callback('pci_remove') do |_, cloud_index|
          cloud = validate_cloud_index(cloud_index)
          next unless cloud

          @manager.remove_cloud(cloud)
          refresh
        end
        @dialog.add_action_callback('pci_apply_preferences') do
          Settings.instance.save!
        end
        @dialog.add_action_callback('pci_set_auto_apply') do |_, enabled|
          self.auto_apply_enabled = enabled.to_s == 'true'
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
              inference: cloud.inference_enabled?,
              octree_debug: cloud.octree_debug_enabled?,
              octree_stats: cloud.last_octree_query_stats,
              color_mode: cloud.color_mode.to_s,
              color_gradient: cloud.color_gradient.to_s,
              single_color: cloud.single_color_hex,
              has_colors: cloud.has_original_colors?,
              has_intensity: cloud.has_intensity?
            }
          end,
          auto_apply: auto_apply_enabled?,
          color_modes: serialize_color_modes,
          gradients: serialize_color_gradients
        }
        @dialog.execute_script("window.pci && window.pci.update(#{JSON.generate(payload)});")
      end

      def validate_cloud_index(index)
        cloud_index = index.to_i
        unless cloud_index.between?(0, @manager.clouds.length - 1)
          warn("[PointCloudImporter] Invalid cloud index: #{index.inspect} (converted to #{cloud_index})")
          return nil
        end

        @manager.clouds[cloud_index]
      end

      def auto_apply_enabled?
        !!@auto_apply_enabled
      end

      def auto_apply_enabled=(value)
        @auto_apply_enabled = !!value
        settings = Settings.instance
        settings[:auto_apply_changes] = @auto_apply_enabled
        settings.save!(:auto_apply_changes, immediate: true)
        Settings.instance.save! if @auto_apply_enabled
        refresh
      end

      def auto_apply_preferences_if_needed
        return unless auto_apply_enabled?

        Settings.instance.save!
      end

      def serialize_color_modes
        PointCloud::COLOR_MODE_DEFINITIONS.map do |definition|
          {
            value: definition[:key].to_s,
            label: definition[:label],
            requires_gradient: !!definition[:gradient],
            requires_single_color: !!definition[:single_color]
          }
        end
      end

      def serialize_color_gradients
        PointCloud::COLOR_GRADIENT_LABELS.map do |key, label|
          { value: key.to_s, label: label }
        end
      end
    end
  end
end
