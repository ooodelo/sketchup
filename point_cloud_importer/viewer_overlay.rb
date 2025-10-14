# frozen_string_literal: true

module PointCloudImporter
  # Overlay responsible for rendering active point clouds efficiently.
  class ViewerOverlay < Sketchup::Overlay
    DRAW_CHUNK = 250_000

    def initialize(manager)
      super('Point Cloud Overlay')
      @manager = manager
    end

    def draw(view)
      @manager.draw(view)
    end

    def on_added(view)
      view.invalidate
    end

    def transparency
      :opaque
    end
  end
end
