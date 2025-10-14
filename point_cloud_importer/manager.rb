# frozen_string_literal: true

require 'singleton'

require_relative 'point_cloud'
require_relative 'viewer_overlay'
require_relative 'measure_tool'

module PointCloudImporter
  # Central registry for loaded point clouds.
  class Manager
    include Singleton

    attr_reader :clouds
    attr_accessor :active_cloud

    def initialize
      @clouds = []
      @overlay = nil
      @active_cloud = nil
    end

    def add_cloud(cloud)
      @clouds << cloud
      @active_cloud = cloud
      ensure_overlay!
      view.invalidate if view
    end

    def remove_cloud(cloud)
      return unless @clouds.delete(cloud)

      cloud.dispose!
      @active_cloud = @clouds.last
      view.invalidate if view
    end

    def clear!
      @clouds.each(&:dispose!)
      @clouds.clear
      @active_cloud = nil
      if @overlay && Sketchup.active_model.respond_to?(:model_overlays)
        Sketchup.active_model.model_overlays.remove(@overlay)
        @overlay = nil
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
      view.invalidate if view
    end

    def view
      Sketchup.active_model&.active_view
    end

    def draw(view)
      @clouds.each do |cloud|
        next unless cloud.visible?

        cloud.draw(view)
      end
    end

    def pick_point(view, x, y)
      cloud = active_cloud
      return unless cloud&.visible?

      ray = view.pickray(x, y)
      return unless ray

      plane_normal = view.camera.direction
      plane_point = cloud.bounding_box.center
      plane = [plane_point, plane_normal]
      projection = Geom.intersect_line_plane(ray, plane)
      return unless projection

      cloud.nearest_point(projection)
    end

    def measurement_tool
      MeasureTool.new(self)
    end
  end
end
