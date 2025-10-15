# frozen_string_literal: true

require_relative 'settings'
require_relative 'settings_buffer'
require_relative 'spatial_index'

module PointCloudImporter
  # Data structure representing a point cloud and display preferences.
  class PointCloud
    attr_reader :name, :points, :colors, :bounding_box
    attr_accessor :visible
    attr_reader :point_size, :point_style

    POINT_STYLE_CANDIDATES = {
      square: %i[DRAW_POINTS_SQUARES DRAW_POINTS_SQUARE DRAW_POINTS_OPEN_SQUARE],
      round: %i[DRAW_POINTS_ROUND DRAW_POINTS_OPEN_CIRCLE],
      plus: %i[DRAW_POINTS_PLUS DRAW_POINTS_CROSS]
    }.freeze

    def initialize(name:, points:, colors: nil, metadata: {})
      @name = name
      @points = points
      @colors = colors
      @metadata = metadata
      @visible = true
      @settings = Settings.instance
      @point_size = @settings[:point_size]
      @point_style = ensure_valid_style(@settings[:point_style])
      persist_style!(@point_style) if @point_style != @settings[:point_style]
      @display_density = @settings[:density]
      @max_display_points = @settings[:max_display_points]
      @display_points = nil
      @display_colors = nil
      @spatial_index = nil
      @inference_group = nil
      compute_bounds!
      build_display_cache!
    end

    def dispose!
      @display_points = nil
      @display_colors = nil
      @spatial_index = nil
      remove_inference_guides!
    end

    def visible?
      @visible
    end

    def point_size=(size)
      @point_size = size.to_i.clamp(1, 10)
      settings = Settings.instance
      settings[:point_size] = @point_size
      SettingsBuffer.instance.write_setting(:point_size, @point_size)
      invalidate_display_cache!
    end

    def point_style=(style)
      style = style.to_sym
      return unless self.class.available_point_style?(style)

      @point_style = style
      persist_style!(@point_style)
    end

    def density=(value)
      value = value.to_f
      value = 0.01 if value <= 0.0
      value = 1.0 if value > 1.0
      @display_density = value
      settings = Settings.instance
      settings[:density] = @display_density
      SettingsBuffer.instance.write_setting(:density, @display_density)
      build_display_cache!
      refresh_inference_guides!
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
      SettingsBuffer.instance.write_setting(:max_display_points, @max_display_points)
      build_display_cache!
      refresh_inference_guides!
    end

    def draw(view)
      build_display_cache! unless @display_points
      return if @display_points.empty?

      view.drawing_color = nil
      view.line_width = 0
      style = self.class.style_constant(@point_style)
      batches do |points_batch, colors_batch|
        if colors_batch
          draw_options = { size: @point_size }
          draw_options[:style] = style if style
          draw_options[:colors] = colors_batch
          view.draw_points(points_batch, **draw_options)
        else
          draw_options = { size: @point_size }
          draw_options[:style] = style if style
          view.draw_points(points_batch, **draw_options)
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

    def inference_enabled?
      valid = @inference_group&.valid?
      @inference_group = nil unless valid
      !!@inference_group
    end

    def toggle_inference_guides!(model)
      if inference_enabled?
        remove_inference_guides!
      else
        ensure_inference_guides!(model)
      end
    end

    def ensure_inference_guides!(model)
      return unless model
      return if inference_enabled?

      build_display_cache! unless @display_points
      return if @display_points.empty?

      guides = sampled_guides
      return if guides.empty?

      model.start_operation('Point Cloud Guides', true)
      begin
        entities = model.entities
        group = entities.add_group
        group.name = "#{name} – направляющие"
        guides.each { |point| group.entities.add_cpoint(point) }
        group.hidden = !visible?
        @inference_group = group
        model.commit_operation
      rescue StandardError
        model.abort_operation
        raise
      end
    end

    def remove_inference_guides!
      group = @inference_group
      @inference_group = nil
      return unless group&.valid?

      model = group.model
      if model
        model.start_operation('Удалить направляющие облака', true)
        begin
          group.erase!
          model.commit_operation
        rescue StandardError
          model.abort_operation
          raise
        end
      end
    end

    def refresh_inference_guides!
      return unless inference_enabled?

      model = Sketchup.active_model
      return unless model

      remove_inference_guides!
      ensure_inference_guides!(model)
    end

    def sync_inference_visibility!
      return unless inference_enabled?

      group = @inference_group
      group.hidden = !visible? if group&.valid?
    end

    private

    DRAW_BATCH_SIZE = 250_000
    MAX_INFERENCE_GUIDES = 50_000

    def sampled_guides
      return [] unless @display_points

      limit = [MAX_INFERENCE_GUIDES, @display_points.length].min
      step = [(@display_points.length.to_f / limit).ceil, 1].max
      guides = []
      @display_points.each_with_index do |point, index|
        next unless (index % step).zero?

        guides << point
        break if guides.length >= limit
      end
      guides
    end

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

    def ensure_valid_style(style)
      style = style.to_sym rescue nil
      return style if style && self.class.available_point_style?(style)

      self.class.default_point_style
    end

    def persist_style!(style)
      return unless self.class.available_point_style?(style)

      settings = Settings.instance
      return if settings[:point_style] == style

      settings[:point_style] = style
      settings.save!(:point_style)
    end

    class << self
      def style_constants
        @style_constants ||= compute_style_constants
      end

      def compute_style_constants
        return {} unless defined?(Sketchup::View)

        POINT_STYLE_CANDIDATES.each_with_object({}) do |(key, candidates), memo|
          memo[key] = resolve_view_constant(candidates)
        end
      end

      def resolve_view_constant(candidates)
        return nil unless defined?(Sketchup::View)

        candidates.each do |const_name|
          next unless view_const_defined?(const_name)

          value = view_const_get(const_name)
          return value if value
        end

        nil
      end

      def view_const_defined?(const_name)
        Sketchup::View.const_defined?(const_name, false)
      rescue NameError
        false
      end

      def view_const_get(const_name)
        Sketchup::View.const_get(const_name, false)
      rescue NameError
        nil
      end

      def available_point_style?(style)
        style = style.to_sym rescue nil
        return false unless style

        !style_constants[style].nil?
      end

      def style_constant(style)
        style = style.to_sym rescue nil
        return unless style

        style_constants[style]
      end

      def available_point_styles
        style_constants.select { |_key, value| value }.keys
      end

      def default_point_style
        available_point_styles.first || :square
      end
    end
  end
end
