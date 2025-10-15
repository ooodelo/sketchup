# frozen_string_literal: true

require 'singleton'
require 'thread'

require_relative 'point_cloud'
require_relative 'viewer_overlay'
require_relative 'measure_tool'

module PointCloudImporter
  # Central registry for loaded point clouds.
  class Manager
    include Singleton

    attr_accessor :active_cloud

    def initialize
      @clouds_lock = Mutex.new
      @clouds = []
      @overlay = nil
      @active_cloud = nil
    end

    def clouds
      @clouds_lock.synchronize { @clouds.dup }
    end

    def add_cloud(cloud)
      @clouds_lock.synchronize do
        @clouds << cloud
        @active_cloud = cloud
        ensure_overlay!
      end
      view.invalidate if view
    end

    def remove_cloud(cloud)
      removed_cloud = nil
      @clouds_lock.synchronize do
        removed_cloud = @clouds.delete(cloud)
        return unless removed_cloud

        @active_cloud = @clouds.last
      end

      removed_cloud.dispose!
      warn_unless_disposed(removed_cloud)
      view.invalidate if view
    end

    def clear!
      clouds_to_dispose = []
      overlay_to_remove = nil
      model = Sketchup.active_model
      supports_overlays = model.respond_to?(:model_overlays)

      @clouds_lock.synchronize do
        clouds_to_dispose = @clouds.dup
        @clouds.clear
        @active_cloud = nil
        if supports_overlays
          overlay_to_remove = @overlay
          @overlay = nil
        end
      end

      clouds_to_dispose.each do |cloud|
        cloud.dispose!
        warn_unless_disposed(cloud)
      end

      if overlay_to_remove
        model.model_overlays.remove(overlay_to_remove)
      end
    end

    def ensure_overlay!
      return unless Sketchup.active_model.respond_to?(:model_overlays)
      return if @overlay

      @overlay = ViewerOverlay.new(self)
      Sketchup.active_model.model_overlays.add(@overlay)
    end

    def toggle_active_visibility
      return unless active_cloud

      active_cloud.visible = !active_cloud.visible?
      active_cloud.sync_inference_visibility!
      view.invalidate if view
    end

    def toggle_active_inference_guides
      cloud = active_cloud
      model = Sketchup.active_model
      return unless cloud && model

      cloud.toggle_inference_guides!(model)
      view.invalidate if view
    end

    def view
      Sketchup.active_model&.active_view
    end

    def draw(view)
      clouds_snapshot = @clouds_lock.synchronize { @clouds.dup }

      clouds_snapshot.each do |cloud|
        next unless cloud.visible?

        cloud.draw(view)
      end
    end

    def pick_point(view, x, y)
      cloud = active_cloud
      return unless cloud&.visible?

      ray = view.pickray(x, y)
      return unless ray

      cloud.closest_point_to_ray(ray, view: view, pixel_tolerance: 10)
    end

    def measurement_tool
      MeasureTool.new(self)
    end

    private

    def warn_unless_disposed(cloud)
      return if cloud.disposed?

      warn("[PointCloudImporter] Облако '#{cloud.name}' не полностью освобождено после dispose!")
    rescue StandardError => e
      warn("[PointCloudImporter] Не удалось проверить состояние облака '#{cloud.name}': #{e.message}")
    end
  end
end
