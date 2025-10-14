# frozen_string_literal: true

require_relative 'settings'
require_relative 'spatial_index'

module PointCloudImporter
  # Data structure representing a point cloud and display preferences.
  class PointCloud
    attr_reader :name, :points, :colors, :bounding_box
    attr_accessor :visible
    attr_reader :point_size, :point_style

    POINT_STYLES = {
      square: Sketchup::View::DRAW_POINTS_SQUARES,
      round: Sketchup::View::DRAW_POINTS_ROUND,
      plus: Sketchup::View::DRAW_POINTS_PLUS
    }.freeze

    def initialize(name:, points:, colors: nil, metadata: {})
      @name = name
      @points = points
      @colors = colors
      @metadata = metadata
      @visible = true
      @settings = Settings.instance
      @point_size = @settings[:point_size]
      @point_style = @settings[:point_style]
      @display_density = @settings[:density]
      @max_display_points = @settings[:max_display_points]
      @display_points = nil
      @display_colors = nil
      @spatial_index = nil
      compute_bounds!
      build_display_cache!
    end

    def dispose!
      @display_points = nil
      @display_colors = nil
      @spatial_index = nil
    end

    def visible?
      @visible
    end

    def point_size=(size)
      @point_size = size.to_i.clamp(1, 10)
      settings = Settings.instance
      settings[:point_size] = @point_size
      settings.save!
      invalidate_display_cache!
    end

    def point_style=(style)
      style = style.to_sym
      return unless POINT_STYLES.key?(style)

      @point_style = style
      settings = Settings.instance
      settings[:point_style] = @point_style
      settings.save!
    end

    def density=(value)
      value = value.to_f
      value = 0.01 if value <= 0.0
      value = 1.0 if value > 1.0
      @display_density = value
      settings = Settings.instance
      settings[:density] = @display_density
      settings.save!
      build_display_cache!
    end

    def density
      @display_density
    end

    def max_display_points=(value)
      available = [points.length, 1].max
      min_allowed = [10_000, available].min
      candidate = value.to_i
      candidate = min_allowed if candidate < min_allowed
      candidate = available if candidate > available

      @max_display_points = candidate
      settings = Settings.instance
      settings[:max_display_points] = @max_display_points
      settings.save!
      build_display_cache!
    end

    def draw(view)
      build_display_cache! unless @display_points
      return if @display_points.empty?

      view.drawing_color = nil
      view.line_width = 0
      style = POINT_STYLES.fetch(@point_style, Sketchup::View::DRAW_POINTS_SQUARES)
      batches do |points_batch, colors_batch|
        if colors_batch
          view.draw_points(points_batch, size: @point_size, style: style, colors: colors_batch)
        else
          view.draw_points(points_batch, size: @point_size, style: style)
        end
      end
    end

    def nearest_point(target)
      build_spatial_index!
      result = @spatial_index.nearest(target)
      return unless result

      points[result[:index]]
    end

    def metadata
      @metadata.dup
    end

    private

    DRAW_BATCH_SIZE = 250_000

    def build_display_cache!
      step = compute_step
      @display_points = []
      @display_colors = colors ? [] : nil

      points.each_with_index do |point, index|
        next unless (index % step).zero?

        @display_points << point
        @display_colors << colors[index] if @display_colors
        break if @display_points.length >= @max_display_points
      end
    end

    def invalidate_display_cache!
      @display_points = nil
      @display_colors = nil
    end

    def compute_step
      return 1 if @display_density >= 0.999

      step = (1.0 / @display_density).round
      step = 1 if step < 1

      estimated = (points.length / step)
      return step if estimated <= @max_display_points

      ((points.length.to_f / @max_display_points).ceil).clamp(1, points.length)
    end

    def batches
      return enum_for(:batches) unless block_given?

      batch_points = []
      batch_colors = @display_colors ? [] : nil
      @display_points.each_with_index do |point, idx|
        batch_points << point
        batch_colors << @display_colors[idx] if batch_colors
        next unless batch_points.length >= DRAW_BATCH_SIZE

        yield(batch_points, batch_colors)
        batch_points = []
        batch_colors = [] if batch_colors
      end
      return if batch_points.empty?

      yield(batch_points, batch_colors)
    end

    def compute_bounds!
      bbox = Geom::BoundingBox.new
      points.each { |pt| bbox.add(pt) }
      @bounding_box = bbox
    end

    def build_spatial_index!
      return if @spatial_index

      sample_target = Settings.instance[:sampling_target]
      sample_step = [(points.length.to_f / sample_target).ceil, 1].max
      sample_points = []
      sample_indices = []
      points.each_with_index do |point, index|
        next unless (index % sample_step).zero?

        sample_points << point
        sample_indices << index
      end
      @spatial_index = SpatialIndex.new(sample_points, sample_indices)
    end
  end
end
