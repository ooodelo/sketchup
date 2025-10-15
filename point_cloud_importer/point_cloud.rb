# frozen_string_literal: true

require 'weakref'

require_relative 'settings'
require_relative 'spatial_index'
require_relative 'octree'

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
      raise ArgumentError, 'Points array cannot be empty' if @points.nil? || @points.empty?
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
      @display_index_lookup = nil
      @spatial_index = nil
      @inference_group = nil
      @octree = nil
      compute_bounds!
      build_display_cache!

      @finalizer_info = self.class.register_finalizer(self,
                                                      name: @name,
                                                      points: @points,
                                                      colors: @colors)
    end

    def dispose!
      clear_cache!
      remove_inference_guides!
      @points = nil
      @colors = nil
      @bounding_box = nil
      mark_finalizer_disposed!
    end

    def clear_cache!
      @display_points = nil
      @display_colors = nil
      @display_index_lookup = nil
      @spatial_index = nil
      @octree = nil
    end

    def visible?
      @visible
    end

    def point_size=(size)
      @point_size = size.to_i.clamp(1, 10)
      settings = Settings.instance
      settings[:point_size] = @point_size
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
      build_display_cache!
      refresh_inference_guides!
    end

    def draw(view)
      build_display_cache! unless @display_points
      return if @display_points.nil? || @display_points.empty?

      view.drawing_color = nil
      view.line_width = 0
      style = self.class.style_constant(@point_style)
      visible_batches(view) do |points_batch, colors_batch|
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

    def closest_point_to_ray(ray, view:, pixel_tolerance: 10)
      build_spatial_index!
      return unless @spatial_index

      origin, direction = ray
      return unless origin && direction

      direction = direction.clone
      return if direction.length.zero?
      direction.normalize!

      tolerance = compute_world_tolerance(view, pixel_tolerance)
      tolerance = [tolerance, MIN_PICK_TOLERANCE].max

      min_point, max_point = expanded_bounds(tolerance)
      segment = ray_bounds_segment(origin, direction, min_point, max_point)
      return unless segment

      entry, exit = segment
      return if exit < 0.0

      entry = 0.0 if entry.negative?
      segment_length = exit - entry

      sample_count = if segment_length <= 0.0
                       1
                     else
                       ((segment_length / tolerance).ceil + 1).clamp(1, MAX_PICK_SAMPLES)
                     end

      sample_step = sample_count > 1 ? (segment_length / (sample_count - 1)) : 0.0
      search_radius = [tolerance, sample_step].max
      tolerance_sq = tolerance * tolerance
      best_point = nil
      best_distance_sq = nil

      sample_count.times do |index|
        distance_along_ray = entry + (sample_step * index)
        sample_point = origin.offset(direction, distance_along_ray)
        candidate = @spatial_index.nearest(sample_point, max_distance: search_radius)
        next unless candidate

        point = points[candidate[:index]]
        projection, distance_sq = projection_and_distance_sq(point, origin, direction)

        next if projection < (entry - tolerance) || projection > (exit + tolerance)
        next if distance_sq > tolerance_sq

        if best_distance_sq.nil? || distance_sq < best_distance_sq
          best_point = point
          best_distance_sq = distance_sq
        end
      end

      best_point
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
          warn_unless_deleted(group)
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

    def apply_point_updates!(changes)
      return unless changes && !changes.empty?

      updates = []

      changes.each do |change|
        index = change[:index]
        next unless index.is_a?(Integer)
        next if index.negative?
        next if index >= points.length

        new_point = change[:point]
        new_color = change[:color]

        points[index] = new_point if new_point
        colors[index] = new_color if colors && !new_color.nil?

        next unless @display_index_lookup

        display_index = @display_index_lookup[index]
        next if display_index.nil?

        updates << { index: display_index, point: new_point, color: new_color }
      end

      update_octree_samples!(updates)
    end

    def caches_cleared?
      @display_points.nil? &&
        @display_colors.nil? &&
        @display_index_lookup.nil? &&
        @spatial_index.nil? &&
        @octree.nil?
    end

    def disposed?
      @points.nil? &&
        @colors.nil? &&
        caches_cleared? &&
        @bounding_box.nil?
    end

    private

    DRAW_BATCH_SIZE = 250_000
    MAX_INFERENCE_GUIDES = 50_000
    MIN_PICK_TOLERANCE = 1e-3
    MAX_PICK_SAMPLES = 4_096

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
      @display_index_lookup = {}

      points.each_with_index do |point, index|
        next unless (index % step).zero?

        @display_points << point
        @display_colors << colors[index] if @display_colors
        @display_index_lookup[index] = @display_points.length - 1
        break if @display_points.length >= @max_display_points
      end

      build_octree_from_display_cache!
    end

    def invalidate_display_cache!
      @display_points = nil
      @display_colors = nil
      @display_index_lookup = nil
      @octree = nil
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

    def visible_batches(view)
      return enum_for(:visible_batches, view) unless block_given?

      unless @octree
        batches do |points_batch, colors_batch|
          yield(points_batch, colors_batch)
        end
        return
      end

      frustum = ViewingFrustum.from_view(view)
      if frustum.nil?
        batches do |points_batch, colors_batch|
          yield(points_batch, colors_batch)
        end
        return
      end

      batch_points = []
      batch_colors = @display_colors ? [] : nil

      @octree.each_leaf_intersecting(frustum) do |node|
        node.indices.each do |index|
          batch_points << @display_points[index]
          batch_colors << @display_colors[index] if batch_colors
          next unless batch_points.length >= DRAW_BATCH_SIZE

          yield(batch_points, batch_colors)
          batch_points = []
          batch_colors = [] if batch_colors
        end
      end

      return if batch_points.empty?

      yield(batch_points, batch_colors)
    end

    def compute_bounds!
      return unless points && !points.empty?

      bbox = Geom::BoundingBox.new
      points.each { |pt| bbox.add(pt) }
      @bounding_box = bbox
    end

    def build_spatial_index!
      return if @spatial_index

      indices = (0...points.length).to_a
      @spatial_index = SpatialIndex.new(points, indices)
    end

    def compute_world_tolerance(view, pixel_tolerance)
      return pixel_tolerance.to_f if view.nil?

      candidate = view.pixels_to_model(pixel_tolerance.to_f, @bounding_box.center)
      if candidate.nil? || candidate <= 0.0
        diagonal = @bounding_box.diagonal.to_f
        viewport = view.respond_to?(:vpwidth) ? view.vpwidth.to_f : 0.0
        if diagonal.positive? && viewport.positive?
          (diagonal / viewport) * pixel_tolerance.to_f
        else
          pixel_tolerance.to_f
        end
      else
        candidate.to_f
      end
    rescue StandardError
      pixel_tolerance.to_f
    end

    def expanded_bounds(distance)
      min_point = @bounding_box.min
      max_point = @bounding_box.max
      expanded_min = Geom::Point3d.new(min_point.x - distance, min_point.y - distance, min_point.z - distance)
      expanded_max = Geom::Point3d.new(max_point.x + distance, max_point.y + distance, max_point.z + distance)
      [expanded_min, expanded_max]
    end

    def ray_bounds_segment(origin, direction, min_point, max_point)
      t_min = -Float::INFINITY
      t_max = Float::INFINITY

      %i[x y z].each do |axis|
        o = origin.public_send(axis)
        d = direction.public_send(axis)
        min_axis = min_point.public_send(axis)
        max_axis = max_point.public_send(axis)

        if d.abs < 1e-9
          return nil if o < min_axis || o > max_axis
          next
        end

        inv = 1.0 / d
        t1 = (min_axis - o) * inv
        t2 = (max_axis - o) * inv
        t1, t2 = t2, t1 if t1 > t2

        t_min = [t_min, t1].max
        t_max = [t_max, t2].min
        return nil if t_min > t_max
      end

      [t_min, t_max]
    end

    def projection_and_distance_sq(point, origin, direction)
      to_point = point - origin
      projection = to_point.dot(direction)
      projection_point = origin.offset(direction, projection)
      diff = point - projection_point
      [projection, diff.dot(diff)]
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
    end

    def build_octree_from_display_cache!
      if @display_points && !@display_points.empty?
        @octree = Octree.new(@display_points, max_points_per_node: Octree::MAX_POINTS_PER_NODE)
      else
        @octree = nil
      end
    end

    def mark_finalizer_disposed!
      return unless @finalizer_info

      @finalizer_info[:disposed] = true
    end

    def warn_unless_deleted(group)
      return if group.deleted?

      warn("[PointCloudImporter] Группа направляющих для '#{@name}' не была удалена полностью")
    rescue StandardError => e
      warn("[PointCloudImporter] Ошибка при проверке удаления направляющих для '#{@name}': #{e.message}")
    end

    def update_octree_samples!(changes)
      return unless changes && !changes.empty?
      return unless @display_points

      indices = []
      changes.each do |change|
        index = change[:index]
        next unless index
        next unless index.is_a?(Integer)
        next if index.negative?
        next if index >= @display_points.length

        point = change[:point]
        color = change[:color]

        if point
          @display_points[index] = point
        end

        if @display_colors && !color.nil?
          @display_colors[index] = color
        end

        indices << index
      end

      return if indices.empty?

      @octree&.update_indices(indices)
    end

    # Viewing frustum constructed from the current SketchUp view.
    class ViewingFrustum
      Plane = Struct.new(:normal, :distance) do
        def distance_to(point)
          (normal.x * point.x) + (normal.y * point.y) + (normal.z * point.z) + distance
        end
      end

      attr_reader :planes

      def self.from_view(view)
        return unless view && view.respond_to?(:camera)

        camera = view.camera
        return unless camera
        return unless camera.respond_to?(:perspective?)
        return unless camera.perspective?

        eye = camera.eye
        direction = camera.direction
        return if direction.length.zero?

        direction.normalize!
        up_vector = camera.respond_to?(:yaxis) ? camera.yaxis : camera.up
        up_vector = Geom::Vector3d.new(0, 0, 1) unless up_vector
        right_vector = if camera.respond_to?(:xaxis)
                         camera.xaxis
                       else
                         direction.cross(up_vector)
                       end

        up = up_vector.clone
        right = right_vector.clone
        up.normalize!
        right.normalize!

        if right.length.zero?
          fallback = Geom::Vector3d.new(1, 0, 0)
          fallback = Geom::Vector3d.new(0, 1, 0) if direction.cross(fallback).length.zero?
          generated = direction.cross(fallback)
          right = generated.length.zero? ? fallback : generated
          right.normalize!
        end

        aspect = view.respond_to?(:vpheight) && view.vpheight.to_f.positive? ? (view.vpwidth.to_f / view.vpheight.to_f) : 1.0
        aspect = 1.0 if aspect.zero? || aspect.nan?

        fov = camera.respond_to?(:fov) ? camera.fov.to_f : 0.0
        return if fov <= 0.0

        fov_rad = fov * Math::PI / 180.0
        near = extract_distance(camera, %i[near znear near_clip], default: 1.0)
        near = 0.1 if near <= 0.0
        far = extract_distance(camera, %i[far zfar far_clip], default: near + 1_000_000.0)
        far = near + 1.0 if far <= near

        near_center = eye.offset(direction, near)
        near_height = Math.tan(fov_rad * 0.5) * near
        near_width = near_height * aspect

        up_near = up.clone
        up_near.length = near_height
        right_near = right.clone
        right_near.length = near_width

        ntl = near_center.offset(up_near - right_near)
        ntr = near_center.offset(up_near + right_near)
        nbl = near_center.offset(-up_near - right_near)
        nbr = near_center.offset(-up_near + right_near)

        far_center = eye.offset(direction, far)

        near_normal = direction.clone
        far_normal = direction.clone
        far_normal.reverse!

        planes = []
        planes << create_plane_from_point(near_normal, near_center)
        planes << create_plane_from_point(far_normal, far_center)
        planes << create_side_plane(nbl, ntl, eye, direction)
        planes << create_side_plane(ntr, nbr, eye, direction)
        planes << create_side_plane(ntr, ntl, eye, direction)
        planes << create_side_plane(nbl, nbr, eye, direction)

        new(planes)
      end

      def initialize(planes)
        @planes = planes.compact
      end

      def intersects_bounds?(bounds)
        return false if bounds.nil?
        return false if @planes.empty?

        corners = bounds.corners
        @planes.each do |plane|
          next unless plane

          outside = corners.all? { |corner| plane.distance_to(corner) < 0.0 }
          return false if outside
        end

        true
      end

      def self.create_plane_from_point(normal, point)
        return unless normal
        return if normal.length.zero?

        normal.normalize!
        distance = -(normal.x * point.x + normal.y * point.y + normal.z * point.z)
        Plane.new(normal, distance)
      end

      def self.create_side_plane(point_a, point_b, eye, forward)
        vector_a = point_a - eye
        vector_b = point_b - eye
        normal = vector_a.cross(vector_b)
        return if normal.length.zero?

        normal.normalize!
        normal.reverse! if normal.dot(forward) < 0.0
        distance = -(normal.x * eye.x + normal.y * eye.y + normal.z * eye.z)
        Plane.new(normal, distance)
      end

      private_class_method :create_plane_from_point
      private_class_method :create_side_plane

      def self.extract_distance(camera, candidates, default: 1.0)
        candidates.each do |name|
          next unless camera.respond_to?(name)

          begin
            value = camera.public_send(name).to_f
            return value if value.positive?
          rescue StandardError
            next
          end
        end

        default
      end

      private_class_method :extract_distance
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

      def register_finalizer(instance, name:, points:, colors:)
        info = {
          name: name,
          disposed: false,
          points_ref: points ? WeakRef.new(points) : nil,
          colors_ref: colors ? WeakRef.new(colors) : nil
        }

        ObjectSpace.define_finalizer(instance, finalizer_proc(info))
        info
      end

      def finalizer_proc(info)
        proc do |_object_id|
          begin
            lingering = lingering_references(info)
            disposed = info[:disposed]
            name = info[:name]

            if !disposed
              detail = lingering.empty? ? 'без остаточных ссылок' : "остатки: #{lingering.join(', ')}"
              warn("[PointCloudImporter] PointCloud '#{name}' уничтожен GC без вызова dispose! (#{detail})")
            elsif lingering.any?
              warn("[PointCloudImporter] PointCloud '#{name}' уничтожен GC с оставшимися ссылками: #{lingering.join(', ')}")
            end
          rescue StandardError => e
            warn("[PointCloudImporter] Ошибка финализатора для облака '#{info[:name]}': #{e.message}")
          end
        end
      end

      def lingering_references(info)
        [].tap do |refs|
          refs << 'points' if weakref_alive?(info[:points_ref])
          refs << 'colors' if weakref_alive?(info[:colors_ref])
        end
      end

      def weakref_alive?(weakref)
        return false unless weakref

        weakref.weakref_alive?
      rescue RefError
        false
      end
    end
  end
end
