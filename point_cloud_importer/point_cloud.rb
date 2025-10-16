# frozen_string_literal: true

require 'weakref'
require 'thread'

require_relative 'settings'
require_relative 'spatial_index'
require_relative 'octree'
require_relative 'chunked_array'
require_relative 'inference_sampler'

module PointCloudImporter
  # Data structure representing a point cloud and display preferences.
  class PointCloud
    attr_reader :name, :points, :colors, :intensities
    attr_accessor :visible
    attr_reader :point_size, :point_style, :inference_sample_indices, :inference_mode
    attr_reader :color_mode, :color_gradient

    POINT_STYLE_CANDIDATES = {
      square: %i[DRAW_POINTS_SQUARES DRAW_POINTS_SQUARE DRAW_POINTS_OPEN_SQUARE],
      round: %i[DRAW_POINTS_ROUND DRAW_POINTS_OPEN_CIRCLE],
      plus: %i[DRAW_POINTS_PLUS DRAW_POINTS_CROSS]
    }.freeze

    LOD_LEVELS = [1.0, 0.5, 0.25, 0.1, 0.05].freeze

    LOD_RULES = [
      { level: 1.0, enter_ratio: 0.0, exit_ratio: 1.5 },
      { level: 0.5, enter_ratio: 1.2, exit_ratio: 2.5 },
      { level: 0.25, enter_ratio: 2.0, exit_ratio: 3.5 },
      { level: 0.1, enter_ratio: 3.0, exit_ratio: 5.0 },
      { level: 0.05, enter_ratio: 4.5, exit_ratio: Float::INFINITY }
    ].freeze

    LOD_FADE_DURATION = 0.35
    BACKGROUND_OCTREE_SAMPLE_LIMIT = 100_000
    BACKGROUND_SPATIAL_SAMPLE_LIMIT = 120_000

    LARGE_POINT_COUNT_THRESHOLD = 3_000_000
    DEFAULT_DISPLAY_POINT_CAP = 1_200_000

    DEFAULT_SINGLE_COLOR_HEX = '#ffffff'

    COLOR_MODE_DEFINITIONS = [
      { key: :original, label: 'Original (PLY)', gradient: false, single_color: false },
      { key: :height, label: 'By Height', gradient: true, single_color: false },
      { key: :intensity, label: 'By Intensity', gradient: true, single_color: false },
      { key: :single, label: 'Single Color', gradient: false, single_color: true },
      { key: :random, label: 'Random', gradient: false, single_color: false },
      { key: :rgb_xyz, label: 'RGB by XYZ', gradient: false, single_color: false }
    ].map { |definition| definition.freeze }.freeze

    COLOR_MODE_LOOKUP = COLOR_MODE_DEFINITIONS.each_with_object({}) do |definition, lookup|
      lookup[definition[:key]] = definition
    end.freeze

    COLOR_GRADIENT_LABELS = {
      viridis: 'Viridis',
      jet: 'Jet',
      grayscale: 'Grayscale',
      magma: 'Magma'
    }.freeze

    COLOR_GRADIENTS = {
      viridis: [
        [0.0, [68, 1, 84]],
        [0.25, [59, 82, 139]],
        [0.5, [33, 145, 140]],
        [0.75, [94, 201, 97]],
        [1.0, [253, 231, 37]]
      ].freeze,
      jet: [
        [0.0, [0, 0, 131]],
        [0.35, [0, 255, 255]],
        [0.5, [255, 255, 0]],
        [0.75, [255, 0, 0]],
        [1.0, [128, 0, 0]]
      ].freeze,
      grayscale: [
        [0.0, [0, 0, 0]],
        [1.0, [255, 255, 255]]
      ].freeze,
      magma: [
        [0.0, [0, 0, 4]],
        [0.25, [78, 18, 123]],
        [0.5, [187, 55, 84]],
        [0.75, [249, 142, 8]],
        [1.0, [251, 252, 191]]
      ].freeze
    }.freeze

    def initialize(name:, points: nil, colors: nil, intensities: nil, metadata: {}, chunk_capacity: ChunkedArray::DEFAULT_CHUNK_CAPACITY)
      @name = name
      @points = ChunkedArray.new(chunk_capacity)
      @colors = colors ? ChunkedArray.new(chunk_capacity) : nil
      @intensities = intensities ? ChunkedArray.new(chunk_capacity) : nil
      @metadata = (metadata || {}).dup
      @visible = true
      @settings = Settings.instance
      @point_size = @settings[:point_size]
      @point_style = ensure_valid_style(@settings[:point_style])
      persist_style!(@point_style) if @point_style != @settings[:point_style]
      @display_density = @settings[:density]
      @max_display_points = @settings[:max_display_points]
      @color_mode = normalize_color_mode(@settings[:color_mode])
      @color_gradient = normalize_color_gradient(@settings[:color_gradient])
      @single_color = parse_color_setting(@settings[:single_color])
      @display_points = nil
      @display_colors = nil
      @display_index_lookup = nil
      @display_point_indices = nil
      @spatial_index = nil
      @inference_group = nil
      @octree = nil
      @octree_mutex = Mutex.new
      @cached_frustum = nil
      @cached_camera_hash = nil
      @last_octree_query_stats = nil
      @octree_debug_enabled = false
      @octree_metadata = nil
      @lod_caches = nil
      @lod_current_level = nil
      @lod_previous_level = nil
      @lod_transition_start = nil
      @lod_background_build_pending = false
      @lod_async_build_timer_id = nil
      @lod_cache_generation = 0
      @lod_background_tasks = nil
      @lod_background_context = nil
      @max_display_point_ramp = nil
      @intensity_min = nil
      @intensity_max = nil
      @random_seed = deterministic_seed_for(name)
      @bounding_box = Geom::BoundingBox.new
      @bounding_box_dirty = false
      @bounds_min_x = nil
      @bounds_min_y = nil
      @bounds_min_z = nil
      @bounds_max_x = nil
      @bounds_max_y = nil
      @bounds_max_z = nil
      @display_cache_dirty = false
      @render_cache_preparation_pending = false
      append_points!(points, colors, intensities) if points && !points.empty?

      @inference_sample_indices = nil
      @inference_mode = nil
      @user_max_display_points = @max_display_points
      build_display_cache! if points && !points.empty?

      @finalizer_info = self.class.register_finalizer(self,
                                                      name: @name,
                                                      points: @points,
                                                      colors: @colors,
                                                      intensities: @intensities)
    end

    def dispose!
      clear_cache!
      remove_inference_guides!
      @points = nil
      @colors = nil
      @intensities = nil
      @intensity_min = nil
      @intensity_max = nil
      @bounding_box = nil
      @bounding_box_dirty = nil
      @bounds_min_x = nil
      @bounds_min_y = nil
      @bounds_min_z = nil
      @bounds_max_x = nil
      @bounds_max_y = nil
      @bounds_max_z = nil
      @octree_mutex = nil
      mark_finalizer_disposed!
    end

    def clear_cache!
      cancel_background_lod_build!
      @display_cache_dirty = true
      @display_points = nil
      @display_colors = nil
      @display_index_lookup = nil
      @display_point_indices = nil
      @spatial_index = nil
      clear_octree!
      reset_frustum_cache!
      @last_octree_query_stats = nil
      @octree_metadata = nil
      @lod_caches = nil
      @lod_current_level = nil
      @lod_previous_level = nil
      @lod_transition_start = nil
      @render_cache_preparation_pending = false
    end

    def visible?
      @visible
    end

    def bounding_box
      rebuild_bounding_box_from_bounds_if_needed!
      @bounding_box
    end

    def finalize_bounds!
      rebuild_bounding_box_from_bounds_if_needed!
      self
    end

    def point_size=(size)
      normalized = size.to_i.clamp(1, 10)
      previous = @point_size
      @point_size = normalized
      settings = Settings.instance
      settings[:point_size] = @point_size
      invalidate_display_cache! if previous != @point_size
    end

    def preview_point_size(size)
      @point_size = size.to_i.clamp(1, 10)
    end

    def point_style=(style)
      style = style.to_sym
      return unless self.class.available_point_style?(style)

      @point_style = style
      persist_style!(@point_style)
    end

    def density=(value)
      normalized = normalize_density(value)
      @display_density = normalized
      settings = Settings.instance
      settings[:density] = @display_density
      invalidate_display_cache!
      refresh_inference_guides!
    end

    def preview_density(value)
      @display_density = normalize_density(value)
    end

    def density
      @display_density
    end

    def color_mode=(mode)
      normalized = normalize_color_mode(mode)
      normalized = :height if normalized == :intensity && !has_intensity?
      return if normalized == @color_mode || normalized.nil?

      @color_mode = normalized
      settings = Settings.instance
      settings[:color_mode] = @color_mode.to_s
      invalidate_display_cache!
      build_display_cache!
    end

    def color_gradient=(gradient)
      normalized = normalize_color_gradient(gradient)
      return if normalized.nil? || normalized == @color_gradient

      @color_gradient = normalized
      settings = Settings.instance
      settings[:color_gradient] = @color_gradient.to_s

      return unless color_mode_requires_gradient?

      invalidate_display_cache!
      build_display_cache!
    end

    def single_color=(value)
      color = parse_color_setting(value)
      return unless color
      return if colors_equal?(@single_color, color)

      @single_color = color
      settings = Settings.instance
      settings[:single_color] = color_to_hex(@single_color)

      return unless color_mode_requires_single_color?

      invalidate_display_cache!
      build_display_cache!
    end

    def single_color
      @single_color ||= parse_color_setting(DEFAULT_SINGLE_COLOR_HEX)
    end

    def has_original_colors?
      @colors && @colors.length.positive?
    end

    def has_intensity?
      @intensities && @intensities.length.positive?
    end

    def export_to_ply(path)
      raise ArgumentError, 'Не указан путь для экспорта.' if path.nil? || path.to_s.strip.empty?
      raise 'Облако не содержит точек.' unless @points && @points.length.positive?

      include_colors = has_original_colors?
      include_intensity = has_intensity?

      File.open(path, 'wb') do |file|
        file << "ply\n"
        file << "format ascii 1.0\n"
        file << "comment Generated by Point Cloud Importer\n"
        file << "element vertex #{@points.length}\n"
        file << "property float x\n"
        file << "property float y\n"
        file << "property float z\n"
        if include_colors
          file << "property uchar red\n"
          file << "property uchar green\n"
          file << "property uchar blue\n"
        end
        file << "property float intensity\n" if include_intensity
        file << "end_header\n"

        @points.each_with_index do |point, index|
          coords = point_coordinates(point)
          next unless coords

          x, y, z = coords
          components = [format('%.6f', x.to_f),
                        format('%.6f', y.to_f),
                        format('%.6f', z.to_f)]

          if include_colors
            color_components = fetch_color_components(@colors && @colors[index])
            color_components ||= [255, 255, 255]
            components.concat(color_components.map(&:to_i))
          end

          if include_intensity
            intensity = @intensities && @intensities[index]
            components << format('%.6f', (intensity || 0.0).to_f)
          end

          file << components.join(' ') << "\n"
        end
      end
    end

    def single_color_hex
      color_to_hex(single_color)
    end

    def octree_debug_enabled?
      !!@octree_debug_enabled
    end

    def octree_debug_enabled=(value)
      @octree_debug_enabled = !!value
    end

    def last_octree_query_stats
      @last_octree_query_stats || {}
    end

    def max_display_points=(value)
      available = [points.length, 1].max
      min_allowed = [10_000, available].min
      candidate = value.to_i
      candidate = min_allowed if candidate < min_allowed
      candidate = available if candidate > available

      @max_display_points = candidate
      @user_max_display_points = @max_display_points
      @max_display_point_ramp = nil
      settings = Settings.instance
      settings[:max_display_points] = @max_display_points
      invalidate_display_cache!
      refresh_inference_guides!
    end

    def prepare_render_cache!
      build_display_cache! if @display_cache_dirty || @lod_caches.nil?
      refresh_inference_guides!
      self
    end

    def render_cache_preparation_pending?
      !!@render_cache_preparation_pending
    end

    def mark_render_cache_preparation_pending!
      return false if @render_cache_preparation_pending

      @render_cache_preparation_pending = true
      true
    end

    def clear_render_cache_preparation_pending!
      @render_cache_preparation_pending = false
    end

    def draw(view)
      build_display_cache! if @display_cache_dirty || @lod_caches.nil?
      ensure_display_caches!
      caches = @lod_caches
      return unless caches && !caches.empty?

      update_active_lod_level(view)
      current_cache = caches[@lod_current_level] || caches[LOD_LEVELS.first]
      return unless current_cache
      return if current_cache[:points].nil? || current_cache[:points].empty?

      view.drawing_color = nil
      view.line_width = 0
      style = self.class.style_constant(@point_style)

      progress = lod_transition_progress
      if @lod_previous_level && @lod_previous_level != @lod_current_level && progress < 1.0
        previous_cache = caches[@lod_previous_level]
        draw_cache(view, previous_cache, 1.0 - progress, style) if previous_cache
        draw_cache(view, current_cache, progress, style)
      else
        draw_cache(view, current_cache, 1.0, style)
      end

      finalize_lod_transition! if progress >= 1.0

      # Optional debug visualization of octree (instance-level)
      if octree_debug_enabled? && @octree && @octree.respond_to?(:draw_debug)
        @octree.draw_debug(view)
      end
    end

    def nearest_point(target)
      build_spatial_index!
      result = @spatial_index.nearest(target)
      return unless result

      result[:point] || point3d_from(points[result[:index]])
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

        point = candidate[:point] || point3d_from(points[candidate[:index]])
        next unless point

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

    def build_static_inference_sample(target_count: nil, progress_callback: nil)
      return unless points && points.length.positive?

      target = target_count || Settings.instance[:static_inference_count]
      target = InferenceSampler::DEFAULT_TARGET_COUNT if target.nil? || target <= 0

      stage_mapping = {
        grid: ['Построение spatial grid', 0.2],
        curvature: ['Вычисление кривизны', 0.5],
        selection: ['Выбор оптимальных точек', 0.8],
        completed: ['Завершено', 1.0]
      }

      sampler_callback = if progress_callback
                            lambda do |stage, progress|
                              mapping = stage_mapping[stage]
                              label = mapping ? mapping[0] : stage.to_s
                              percent = progress
                              percent = mapping[1] if percent.nil?
                              progress_callback.call(label, percent)
                            end
                          end

      sampler = InferenceSampler.new(points, target, progress_callback: sampler_callback)
      indices = sampler.compute_sample
      return [] if indices.empty?

      @inference_sample_indices = indices
      @inference_mode = :static
      create_inference_guides(indices, mode: :static)

      indices
    end

    def ensure_inference_guides!(model)
      return unless model
      return if inference_enabled?

      guides = inference_guides_from_sample
      if guides.empty?
        build_display_cache! unless @display_points
        return if @display_points.empty?

        guides = sampled_guides
      end
      return if guides.empty?

      model.start_operation('Point Cloud Guides', true)
      begin
        entities = model.entities
        group = entities.add_group
        group.name = inference_group_name
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

    def create_inference_guides(indices, model: nil, mode: nil)
      return unless indices && !indices.empty?

      model ||= Sketchup.active_model
      return unless model

      points_for_guides = indices.each_with_object([]) do |index, collection|
        next unless index.is_a?(Integer) && index >= 0 && index < points.length

        point = point3d_from(points[index])
        collection << point if point
      end
      return if points_for_guides.empty?

      remove_inference_guides!

      model.start_operation('Point Cloud Guides', true)
      begin
        group = model.entities.add_group
        group.name = inference_group_name(mode)
        points_for_guides.each { |point| group.entities.add_cpoint(point) }
        group.hidden = !visible?
        @inference_group = group
        @inference_mode = mode if mode
        model.commit_operation
      rescue StandardError
        model.abort_operation
        raise
      end
    end

    def inference_group_name(mode = @inference_mode)
      suffix = case mode
               when :static then 'направляющие (Static)'
               when :dynamic then 'направляющие (Dynamic)'
               else 'направляющие'
               end

      "Облако #{name} - #{suffix}"
    end

    def inference_guides_from_sample
      return [] unless @inference_sample_indices && !@inference_sample_indices.empty?

      @inference_sample_indices.each_with_object([]) do |index, guides|
        next unless index.is_a?(Integer)

        point = point3d_from(points[index])
        guides << point if point
      end
    end

    def apply_point_updates!(changes)
      return unless changes && !changes.empty?

      changes.each do |change|
        index = change[:index]
        next unless index.is_a?(Integer)
        next if index.negative?
        next if index >= points.length

        new_point = change[:point]
        new_color = change[:color]
        new_intensity = change[:intensity]

        if new_point
          coords = point_coordinates(new_point)
          stored_point = coords ? [coords[0], coords[1], coords[2]] : new_point
          points[index] = stored_point
          update_bounds_with_chunk([stored_point])
        end

        if colors && !new_color.nil?
          colors[index] = fetch_color_components(new_color) || new_color
        end
        if @intensities && !new_intensity.nil?
          @intensities[index] = new_intensity
          update_intensity_range!([new_intensity])
        end
      end

      mark_display_cache_dirty!
    end

    def caches_cleared?
      @display_points.nil? &&
      @display_colors.nil? &&
      @display_index_lookup.nil? &&
      @display_point_indices.nil? &&
      @spatial_index.nil? &&
        @octree.nil? &&
        @octree_metadata.nil? &&
        @cached_frustum.nil? &&
        @cached_camera_hash.nil? &&
        (@last_octree_query_stats.nil? || @last_octree_query_stats.empty?) &&
        (@lod_caches.nil? || @lod_caches.empty?)
    end

    def disposed?
      @points.nil? &&
        @colors.nil? &&
        caches_cleared? &&
        @bounding_box.nil?
    end

    def set_points_bulk!(points_array, colors_array = nil, intensities_array = nil)
      points_array ||= []
      colors_array = nil if colors_array&.empty?
      intensities_array = nil if intensities_array&.empty?

      reset_bounds_state!
      @points.clear
      @points.append_chunk(points_array)

      if colors_array
        @colors ||= ChunkedArray.new(@points.chunk_capacity)
        @colors.clear
        @colors.append_chunk(colors_array)
      else
        @colors = nil
      end

      if intensities_array
        @intensities ||= ChunkedArray.new(@points.chunk_capacity)
        @intensities.clear
        @intensity_min = nil
        @intensity_max = nil
        @intensities.append_chunk(intensities_array)
        update_intensity_range!(intensities_array)
      else
        @intensities = nil
        @intensity_min = nil
        @intensity_max = nil
      end

      compute_bounds_efficient!(points_array)
      finalize_bounds!
      invalidate_display_cache!
      self
    end

    def append_points!(points_chunk, colors_chunk = nil, intensities_chunk = nil)
      return if points_chunk.nil? || points_chunk.empty?

      @points.append_chunk(points_chunk)
      if colors_chunk && !colors_chunk.empty?
        @colors ||= ChunkedArray.new(@points.chunk_capacity)
        @colors.append_chunk(colors_chunk)
      end
      if intensities_chunk && !intensities_chunk.empty?
        @intensities ||= ChunkedArray.new(@points.chunk_capacity)
        @intensities.append_chunk(intensities_chunk)
        update_intensity_range!(intensities_chunk)
      end

      update_bounds_with_chunk(points_chunk)

      mark_display_cache_dirty!
    end

    def update_metadata!(metadata)
      @metadata = (metadata || {}).dup
    end

    private

    def mark_display_cache_dirty!
      return if @display_cache_dirty

      invalidate_display_cache!
    end

    def rebuild_bounding_box_from_bounds_if_needed!
      return unless @bounding_box_dirty || @bounding_box.nil?

      rebuild_bounding_box_from_bounds!
    end

    def reset_bounds_state!
      @bounds_min_x = nil
      @bounds_min_y = nil
      @bounds_min_z = nil
      @bounds_max_x = nil
      @bounds_max_y = nil
      @bounds_max_z = nil
      @bounding_box = Geom::BoundingBox.new
      @bounding_box_dirty = false
    end

    def rebuild_bounding_box_from_bounds!
      if [@bounds_min_x, @bounds_min_y, @bounds_min_z,
          @bounds_max_x, @bounds_max_y, @bounds_max_z].any?(&:nil?)
        @bounding_box = Geom::BoundingBox.new
        @bounding_box_dirty = false
        return
      end

      bbox = Geom::BoundingBox.new
      bbox.add(Geom::Point3d.new(@bounds_min_x, @bounds_min_y, @bounds_min_z))
      bbox.add(Geom::Point3d.new(@bounds_max_x, @bounds_max_y, @bounds_max_z))
      @bounding_box = bbox
      @bounding_box_dirty = false
    end

    def update_bounds_with_chunk(points_chunk)
      return unless points_chunk && !points_chunk.empty?

      chunk_min_x = Float::INFINITY
      chunk_min_y = Float::INFINITY
      chunk_min_z = Float::INFINITY
      chunk_max_x = -Float::INFINITY
      chunk_max_y = -Float::INFINITY
      chunk_max_z = -Float::INFINITY

      points_chunk.each do |point|
        coords = point_coordinates(point)
        next unless coords

        x, y, z = coords
        chunk_min_x = x if x < chunk_min_x
        chunk_max_x = x if x > chunk_max_x
        chunk_min_y = y if y < chunk_min_y
        chunk_max_y = y if y > chunk_max_y
        chunk_min_z = z if z < chunk_min_z
        chunk_max_z = z if z > chunk_max_z
      end

      return if chunk_min_x == Float::INFINITY

      if @bounds_min_x.nil?
        @bounds_min_x = chunk_min_x
        @bounds_min_y = chunk_min_y
        @bounds_min_z = chunk_min_z
        @bounds_max_x = chunk_max_x
        @bounds_max_y = chunk_max_y
        @bounds_max_z = chunk_max_z
      else
        @bounds_min_x = [@bounds_min_x, chunk_min_x].min
        @bounds_min_y = [@bounds_min_y, chunk_min_y].min
        @bounds_min_z = [@bounds_min_z, chunk_min_z].min
        @bounds_max_x = [@bounds_max_x, chunk_max_x].max
        @bounds_max_y = [@bounds_max_y, chunk_max_y].max
        @bounds_max_z = [@bounds_max_z, chunk_max_z].max
      end

      @bounding_box_dirty = true
    end

    AXIS_INDICES = { x: 0, y: 1, z: 2 }.freeze

    def point_coordinate(point, axis)
      return nil unless point

      if point.respond_to?(axis)
        value = point.public_send(axis)
      elsif point.respond_to?(:[]) && AXIS_INDICES.key?(axis)
        value = point[AXIS_INDICES[axis]]
      end

      return nil if value.nil?

      value.to_f
    rescue StandardError
      nil
    end

    def point_coordinates(point)
      return nil unless point

      if point.respond_to?(:x) && point.respond_to?(:y) && point.respond_to?(:z)
        [point.x.to_f, point.y.to_f, point.z.to_f]
      elsif point.respond_to?(:[])
        x = point[0]
        y = point[1]
        z = point[2]
        return nil if x.nil? || y.nil? || z.nil?

        [x.to_f, y.to_f, z.to_f]
      else
        nil
      end
    rescue StandardError
      nil
    end

    def point3d_from(point)
      return point if point.is_a?(Geom::Point3d)

      coords = point_coordinates(point)
      return nil unless coords

      Geom::Point3d.new(coords[0], coords[1], coords[2])
    rescue StandardError
      nil
    end

    def fetch_color_components(stored)
      return nil unless stored

      if stored.respond_to?(:red) && stored.respond_to?(:green) && stored.respond_to?(:blue)
        [stored.red.to_i, stored.green.to_i, stored.blue.to_i]
      elsif stored.is_a?(Array)
        r = stored[0]
        g = stored[1]
        b = stored[2]
        return nil if r.nil? || g.nil? || b.nil?

        [r.to_i, g.to_i, b.to_i]
      else
        nil
      end
    rescue StandardError
      nil
    end

    def color_from_storage(stored)
      components = fetch_color_components(stored)
      return nil unless components

      Sketchup::Color.new(components[0], components[1], components[2])
    rescue StandardError
      nil
    end

    DRAW_BATCH_SIZE = 250_000
    MAX_INFERENCE_GUIDES = 50_000
    MIN_PICK_TOLERANCE = 1e-3
    MAX_PICK_SAMPLES = 4_096

    def sampled_guides
      return [] unless @display_points

      limit = [MAX_INFERENCE_GUIDES, @display_points.length].min
      step = [(@display_points.length.to_f / limit).ceil, 1].max
      guides = []
      (0...@display_points.length).step(step) do |index|
        guides << @display_points[index]
        break if guides.length >= limit
      end
      guides
    end

    def color_buffer_enabled?
      return false if @color_mode == :original && !has_original_colors?

      true
    end

    def display_color_for_index(original_index)
      case @color_mode
      when :original
        original_color_at(original_index)
      when :height
        point = @points[original_index]
        point ? gradient_color(normalize_height(point)) : nil
      when :intensity
        intensity = intensity_at(original_index)
        if !intensity.nil?
          gradient_color(normalize_intensity(intensity))
        else
          point = @points[original_index]
          point ? gradient_color(normalize_height(point)) : nil
        end
      when :single
        Sketchup::Color.new(single_color)
      when :random
        random_color_for_index(original_index)
      when :rgb_xyz
        point = @points[original_index]
        point ? rgb_from_xyz(point) : nil
      else
        original_color_at(original_index)
      end
    end

    def original_color_at(index)
      return nil unless @colors && index

      color = color_from_storage(@colors[index])
      color || Sketchup::Color.new(255, 255, 255)
    rescue StandardError
      Sketchup::Color.new(255, 255, 255)
    end

    def gradient_color(value)
      stops = COLOR_GRADIENTS[@color_gradient] || COLOR_GRADIENTS[:viridis]
      return Sketchup::Color.new(*stops.last[1]) unless stops && !stops.empty?

      t = clamp01(value.to_f)
      lower = stops.first
      upper = stops.last

      stops.each do |stop|
        if t <= stop[0]
          upper = stop
          break
        end
        lower = stop
      end

      if upper == lower
        components = lower[1]
        return Sketchup::Color.new(components[0], components[1], components[2])
      end

      range = upper[0] - lower[0]
      weight = range.abs < Float::EPSILON ? 0.0 : (t - lower[0]) / range
      interpolated = lower[1].zip(upper[1]).map do |min_component, max_component|
        (min_component + (max_component - min_component) * weight).round.clamp(0, 255)
      end
      Sketchup::Color.new(interpolated[0], interpolated[1], interpolated[2])
    end

    def clamp01(value)
      [[value, 0.0].max, 1.0].min
    rescue StandardError
      0.0
    end

    def normalize_color_mode(mode)
      return @color_mode if mode.nil?

      key = case mode
            when String then mode.strip.downcase.to_sym
            else mode.to_sym
            end

      COLOR_MODE_LOOKUP[key] ? key : :original
    rescue StandardError
      :original
    end

    def normalize_color_gradient(gradient)
      return @color_gradient if gradient.nil?

      key = case gradient
            when String then gradient.strip.downcase.to_sym
            else gradient.to_sym
            end

      COLOR_GRADIENTS.key?(key) ? key : COLOR_GRADIENTS.keys.first
    rescue StandardError
      COLOR_GRADIENTS.keys.first
    end

    def parse_color_setting(value)
      candidate = case value
                  when Sketchup::Color
                    value
                  when String
                    value.strip.empty? ? DEFAULT_SINGLE_COLOR_HEX : value
                  else
                    value
                  end

      color = Sketchup::Color.new(candidate || DEFAULT_SINGLE_COLOR_HEX)
      Sketchup::Color.new(color)
    rescue StandardError
      Sketchup::Color.new(DEFAULT_SINGLE_COLOR_HEX)
    end

    def color_to_hex(color)
      return DEFAULT_SINGLE_COLOR_HEX unless color

      format('#%02x%02x%02x', color.red.to_i.clamp(0, 255), color.green.to_i.clamp(0, 255), color.blue.to_i.clamp(0, 255))
    rescue StandardError
      DEFAULT_SINGLE_COLOR_HEX
    end

    def colors_equal?(first, second)
      return true if first.equal?(second)
      return false unless first && second

      first.red == second.red && first.green == second.green && first.blue == second.blue
    rescue StandardError
      false
    end

    def color_mode_requires_gradient?(mode = @color_mode)
      definition = COLOR_MODE_LOOKUP[mode]
      definition ? !!definition[:gradient] : false
    end

    def color_mode_requires_single_color?(mode = @color_mode)
      definition = COLOR_MODE_LOOKUP[mode]
      definition ? !!definition[:single_color] : false
    end

    def deterministic_seed_for(value)
      seed = 0x811c9dc5
      value.to_s.each_byte do |byte|
        seed ^= byte
        seed = (seed * 0x01000193) & 0xffffffff
      end
      seed
    rescue StandardError
      0x12345678
    end

    def random_color_for_index(index)
      seed = (@random_seed + index.to_i * 1_103_515_245 + 12_345) & 0xffffffff
      r = (((seed >> 16) & 0xff) / 2.0 + 64).round.clamp(0, 255)
      g = (((seed >> 8) & 0xff) / 2.0 + 64).round.clamp(0, 255)
      b = ((seed & 0xff) / 2.0 + 64).round.clamp(0, 255)
      Sketchup::Color.new(r, g, b)
    rescue StandardError
      Sketchup::Color.new(255, 255, 255)
    end

    def intensity_at(index)
      return nil unless @intensities && index

      value = @intensities[index]
      value&.to_f
    rescue StandardError
      nil
    end

    def normalize_intensity(value)
      min = @intensity_min
      max = @intensity_max
      return 0.0 if min.nil? || max.nil?

      range = max - min
      return 0.0 if range.abs < Float::EPSILON

      clamp01((value.to_f - min) / range)
    rescue StandardError
      0.0
    end

    def normalize_height(point)
      bbox = bounding_box
      return 0.5 unless bbox

      z = point_coordinate(point, :z)
      return 0.5 if z.nil?

      min_z = bbox.min.z
      max_z = bbox.max.z
      range = max_z - min_z
      return 0.5 if range.abs < Float::EPSILON

      clamp01((z.to_f - min_z) / range)
    rescue StandardError
      0.5
    end

    def rgb_from_xyz(point)
      bbox = bounding_box
      return Sketchup::Color.new(255, 255, 255) unless bbox

      coords = point_coordinates(point)
      return Sketchup::Color.new(255, 255, 255) unless coords

      min = bbox.min
      max = bbox.max

      x = normalize_component(coords[0], min.x, max.x)
      y = normalize_component(coords[1], min.y, max.y)
      z = normalize_component(coords[2], min.z, max.z)

      Sketchup::Color.new((x * 255).round.clamp(0, 255),
                          (y * 255).round.clamp(0, 255),
                          (z * 255).round.clamp(0, 255))
    rescue StandardError
      Sketchup::Color.new(255, 255, 255)
    end

    def normalize_component(value, minimum, maximum)
      range = maximum - minimum
      return 0.5 if range.abs < Float::EPSILON

      clamp01((value.to_f - minimum) / range)
    rescue StandardError
      0.5
    end

    def update_intensity_range!(values)
      return unless values

      values.each do |raw|
        next unless raw.respond_to?(:to_f)

        value = raw.to_f
        next unless value.finite?

        @intensity_min = value if @intensity_min.nil? || value < @intensity_min
        @intensity_max = value if @intensity_max.nil? || value > @intensity_max
      end
    end

    def build_display_cache!
      finalize_bounds!
      @display_cache_dirty = false
      @spatial_index = nil
      cancel_background_lod_build!
      clear_octree!
      reset_frustum_cache!
      @last_octree_query_stats = nil
      @octree_metadata = nil
      @lod_caches = {}
      @lod_cache_generation = (@lod_cache_generation || 0) + 1
      current_generation = @lod_cache_generation

      return unless points && !points.empty?

      target_limit = @user_max_display_points.to_i
      if target_limit <= 0
        target_limit = @settings[:max_display_points].to_i
      end
      target_limit = DEFAULT_DISPLAY_POINT_CAP if target_limit <= 0

      available_points = points.length
      if available_points.positive? && target_limit > available_points
        target_limit = available_points
      end

      @user_max_display_points = target_limit
      @max_display_points = target_limit

      startup_cap = [@max_display_points, 250_000].min
      startup_cap = available_points if startup_cap <= 0 && available_points.positive?
      startup_cap = 250_000 if startup_cap <= 0

      base_cache = build_base_cache(limit: startup_cap)

      if base_cache
        atomic_swap_lod!(0, base_cache)
      else
        assign_primary_cache(nil)
      end
      @lod_current_level ||= LOD_LEVELS.first
      @lod_previous_level = nil
      @lod_transition_start = nil

      if target_limit.positive? && target_limit > startup_cap && startup_cap.positive?
        @max_display_points = startup_cap
      end

      start_background_lod_build(target_limit, current_generation, startup_cap)
    end

    def ensure_display_caches!
      build_display_cache! unless @lod_caches

      if @display_points.nil?
        base_cache = base_lod_cache
        assign_primary_cache(base_cache)
        if base_cache && (!build_octree_async? || base_cache[:octree])
          ensure_octree_for_cache(base_cache)
        end
      end
    end

    def assign_primary_cache(cache)
      mutex = (@octree_mutex ||= Mutex.new)

      if cache
        @display_points = cache[:points]
        @display_colors = cache[:colors]
        @display_index_lookup = cache[:index_lookup]
        @display_point_indices = cache[:point_indices]
        mutex.synchronize do
          @octree = cache[:octree]
          @octree_metadata = cache[:octree_metadata]
        end
      else
        @display_points = nil
        @display_colors = nil
        @display_index_lookup = nil
        @display_point_indices = nil
        mutex.synchronize do
          @octree = nil
          @octree_metadata = nil
        end
      end
    end

    def create_cache(level, points, colors, index_lookup, point_indices)
      cache_points = points ? points : []
      cache_colors = colors.nil? ? nil : colors
      cache_index_lookup = index_lookup ? index_lookup : {}
      cache_point_indices = point_indices ? point_indices : []

      cache = {
        level: level,
        points: cache_points,
        colors: cache_colors,
        index_lookup: cache_index_lookup,
        point_indices: cache_point_indices,
        octree: nil,
        octree_metadata: nil
      }

      cache.define_singleton_method(:size) do
        pts = self[:points]
        pts ? pts.length : 0
      end

      cache.define_singleton_method(:length) do
        size
      end

      cache
    end

    def build_base_cache(limit:)
      return nil unless points && !points.empty?

      step = compute_step(limit: limit)
      base_points = []
      base_colors = color_buffer_enabled? ? [] : nil
      base_index_lookup = {}
      base_point_indices = []
      max_points = limit.to_i.positive? ? limit.to_i : nil

      (0...points.length).step(step) do |index|
        point = point3d_from(points[index])
        next unless point

        base_points << point
        base_colors << display_color_for_index(index) if base_colors
        base_index_lookup[index] = base_points.length - 1
        base_point_indices << index

        break if max_points && base_points.length >= max_points
      end

      create_cache(LOD_LEVELS.first,
                   base_points,
                   base_colors,
                   base_index_lookup,
                   base_point_indices)
    end

    def store_lod_cache(level, cache)
      return unless cache

      @lod_caches ||= {}
      key = lod_level_key(level)
      @lod_caches[key] = cache
      if level == 0 || key == LOD_LEVELS.first
        @lod_caches[LOD_LEVELS.first] = cache
        @lod_caches[0] = cache
      end
      cache
    end

    def lod_level_key(level)
      return LOD_LEVELS.first if level == 0

      level
    end

    def base_lod_cache
      return nil unless @lod_caches

      @lod_caches[LOD_LEVELS.first] || @lod_caches[0]
    end

    def cache_size(cache)
      return 0 unless cache

      points = cache[:points]
      points ? points.length : 0
    end

    def base_cache_point_count
      cache_size(base_lod_cache)
    end

    def create_downsampled_cache(base_cache, level)
      return base_cache if base_cache.nil? || level >= 0.999

      points = []
      colors = base_cache[:colors] ? [] : nil
      index_lookup = {}
      point_indices = []

      step = (1.0 / level).round
      step = 1 if step < 1

      base_points = base_cache[:points] || []
      base_indices = base_cache[:point_indices] || []

      base_indices.each_with_index do |original_index, base_position|
        next unless (base_position % step).zero?

        points << base_points[base_position]
        colors << base_cache[:colors][base_position] if colors && base_cache[:colors]
        index_lookup[original_index] = points.length - 1
        point_indices << original_index
      end

      create_cache(level, points, colors, index_lookup, point_indices)
    end

    def start_background_lod_build(target_limit, generation, startup_cap)
      context = {
        generation: generation,
        target_limit: target_limit.to_i,
        startup_cap: startup_cap.to_i,
        needs_ramp: target_limit.to_i.positive? && target_limit > startup_cap && startup_cap.positive?
      }

      steps = prepare_background_steps(context)

      @lod_background_context = context
      @lod_background_tasks = steps.dup
      @max_display_point_ramp = nil

      queue_empty = @lod_background_tasks.empty? && !context[:needs_ramp]
      unless queue_empty
        @lod_background_build_pending = true
      end

      maybe_start_max_display_point_ramp(context)

      if queue_empty
        finalize_background_build(context)
        return
      end

      run_steps_as_ui_timer(context)
    end

    def prepare_background_steps(context)
      generation = context[:generation]
      target_limit = context[:target_limit]

      steps = []
      steps << lambda { expand_lod0_to!(target_limit, generation) }
      LOD_LEVELS[1..-1].each do |level|
        steps << lambda { rebuild_lod!(level, generation) }
      end
      steps << lambda { build_octree_sampled(generation) }
      steps << lambda { build_spatial_index_sampled(generation) }
      steps
    end

    def run_steps_as_ui_timer(context)
      if defined?(UI) && UI.respond_to?(:start_timer)
        timer_id = nil
        timer_id = UI.start_timer(0, true) do
          continue = run_background_step_slice(context)
          next if continue

          if defined?(UI) && UI.respond_to?(:stop_timer)
            UI.stop_timer(timer_id)
          end

          @lod_async_build_timer_id = nil
        end

        @lod_async_build_timer_id = timer_id
      else
        while run_background_step_slice(context)
        end

        @lod_async_build_timer_id = nil
      end
    end

    def run_background_step_slice(context)
      unless context && context[:generation] == @lod_cache_generation
        finalize_background_build(context)
        return false
      end

      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      while @lod_background_tasks && !@lod_background_tasks.empty?
        step = @lod_background_tasks.shift
        execute_background_step(step, context)
        throttled_invalidate_view

        break if (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time) >= 0.02
      end

      maybe_start_max_display_point_ramp(context)
      ramp_active = continue_max_display_point_ramp

      if (@lod_background_tasks.nil? || @lod_background_tasks.empty?) && !ramp_active
        finalize_background_build(context)
        return false
      end

      true
    rescue StandardError => error
      warn_background_build_failure(error)
      finalize_background_build(context)
      false
    end

    def execute_background_step(step, context)
      return unless step
      return unless context[:generation] == @lod_cache_generation

      step.call if step.respond_to?(:call)
    end

    def expand_lod0_to!(target_limit, generation)
      return unless generation == @lod_cache_generation

      target_limit = target_limit.to_i
      return if target_limit <= 0

      base_cache = base_lod_cache
      return unless base_cache

      current_size = cache_size(base_cache)
      return if current_size >= target_limit

      new_cache = build_base_cache(limit: target_limit)
      return unless new_cache

      atomic_swap_lod!(0, new_cache)
    end

    def rebuild_lod!(level, generation)
      return unless generation == @lod_cache_generation

      base_cache = base_lod_cache
      return unless base_cache

      new_cache = create_downsampled_cache(base_cache, level)
      atomic_swap_lod!(level, new_cache) if new_cache
    end

    def atomic_swap_lod!(level, cache)
      stored_cache = store_lod_cache(level, cache)
      return unless stored_cache

      current_level = @lod_current_level || LOD_LEVELS.first
      cache_level = lod_level_key(level)

      assign_primary_cache(stored_cache) if cache_level == current_level

      throttled_invalidate_view

      stored_cache
    end

    def throttled_invalidate_view
      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      last = (@_last_invalidate_at ||= 0.0)
      return if (now - last) < 0.2

      @_last_invalidate_at = now
      view = @manager&.view
      if view && view.respond_to?(:invalidate)
        view.invalidate
      else
        invalidate_active_view
      end
    rescue StandardError
      nil
    end

    def build_octree_sampled(generation)
      return unless generation == @lod_cache_generation

      base_cache = base_lod_cache
      return unless base_cache

      points = base_cache[:points]
      return unless points && !points.empty?

      sampled_points, = sample_cache_points(base_cache, BACKGROUND_OCTREE_SAMPLE_LIMIT)
      return if sampled_points.nil? || sampled_points.empty?

      built_octree = build_octree_for_points(sampled_points)
      return unless built_octree

      metadata = {
        sampled: true,
        sample_size: sampled_points.length,
        max_display_points_snapshot: @max_display_points
      }

      mutex = (@octree_mutex ||= Mutex.new)
      mutex.synchronize do
        return unless generation == @lod_cache_generation

        base_cache[:octree] = built_octree
        base_cache[:octree_metadata] = metadata

        current_cache = base_lod_cache
        if base_cache.equal?(current_cache)
          @octree = built_octree
          @octree_metadata = metadata
        end
      end
    end

    def build_spatial_index_sampled(generation)
      return unless generation == @lod_cache_generation

      base_cache = base_lod_cache
      return unless base_cache

      sampled_points, sampled_indices = sample_cache_points(base_cache, BACKGROUND_SPATIAL_SAMPLE_LIMIT)
      return if sampled_points.nil? || sampled_points.empty?
      return if sampled_indices.nil? || sampled_indices.empty?

      spatial_index = SpatialIndex.new(sampled_points, sampled_indices)
      return unless spatial_index

      if generation == @lod_cache_generation
        @spatial_index = spatial_index
      end
    end

    def sample_cache_points(cache, max_samples)
      points = cache[:points] || []
      indices = cache[:point_indices] || []

      return [points, indices] if max_samples.to_i <= 0
      return [points, indices] if points.length <= max_samples

      step = (points.length.to_f / max_samples).ceil
      step = 1 if step < 1

      sampled_points = []
      sampled_indices = []

      points.each_with_index do |point, index|
        next unless (index % step).zero?

        sampled_points << point
        sampled_index = indices[index] || index
        sampled_indices << sampled_index
      end

      [sampled_points, sampled_indices]
    end

    def maybe_start_max_display_point_ramp(context)
      return unless context
      return unless context[:needs_ramp]
      return if @max_display_point_ramp

      target_limit = context[:target_limit].to_i
      return if target_limit <= 0

      effective_target = effective_ramp_target(target_limit)
      return if effective_target <= 0

      current_limit = @max_display_points.to_i
      return if current_limit >= effective_target

      @max_display_point_ramp = { target: target_limit }
    end

    def continue_max_display_point_ramp
      ramp = @max_display_point_ramp
      return false unless ramp

      target_limit = ramp[:target].to_i
      effective_target = effective_ramp_target(target_limit)
      return finalize_ramp if effective_target <= 0

      current_limit = @max_display_points.to_i
      if current_limit >= effective_target
        return true if effective_target < target_limit

        finalize_ramp
        return false
      end

      effective_difference = effective_target - current_limit
      return finalize_ramp if effective_difference <= 0

      increment = compute_max_display_point_increment(effective_target, current_limit)
      increment = effective_difference if increment > effective_difference
      increment = 1 if increment <= 0

      new_limit = current_limit + increment
      new_limit = target_limit if new_limit > target_limit
      new_limit = effective_target if new_limit > effective_target

      @max_display_points = new_limit
      throttled_invalidate_view

      if new_limit >= target_limit
        finalize_ramp
        return false
      end

      true
    end

    def compute_max_display_point_increment(target_limit, current_limit)
      difference = target_limit - current_limit
      return difference if difference <= 0

      increment = (difference * 0.25).ceil
      increment = 25_000 if increment < 25_000
      increment
    end

    def finalize_ramp
      if @max_display_point_ramp
        target_limit = @max_display_point_ramp[:target].to_i
        effective_target = effective_ramp_target(target_limit)
        if effective_target.positive?
          if target_limit.positive?
            @max_display_points = [target_limit, effective_target].min
          else
            @max_display_points = effective_target
          end
        end
      end

      @max_display_point_ramp = nil
      false
    end

    def effective_ramp_target(target_limit)
      base_size = base_cache_point_count
      return target_limit.to_i if base_size <= 0

      [target_limit.to_i, base_size].min
    end

    def invalidate_active_view
      return unless defined?(Sketchup)

      model = Sketchup.active_model
      return unless model

      view = model.respond_to?(:active_view) ? model.active_view : nil
      return unless view && view.respond_to?(:invalidate)

      view.invalidate
    rescue StandardError
      nil
    end

    def finalize_background_build(context)
      if context && context[:generation] == @lod_cache_generation
        target_limit = context[:target_limit].to_i
        if target_limit.positive? && @max_display_points < target_limit
          @max_display_points = target_limit
        end

        @lod_background_build_pending = false
      end

      if @lod_async_build_timer_id && defined?(UI) && UI.respond_to?(:stop_timer)
        UI.stop_timer(@lod_async_build_timer_id)
      end

      @lod_background_tasks = nil
      @lod_background_context = nil
      @max_display_point_ramp = nil
      @lod_async_build_timer_id = nil
      @lod_background_build_pending = false
    end

    def build_spatial_index_for_cache(cache)
      return nil unless cache

      points = cache[:points]
      indices = cache[:point_indices]

      return nil unless points && indices
      return nil if points.empty? || indices.empty?

      SpatialIndex.new(points, indices)
    end

    def cancel_background_lod_build!
      if @lod_async_build_timer_id && defined?(UI) && UI.respond_to?(:stop_timer)
        UI.stop_timer(@lod_async_build_timer_id)
      end

      @lod_async_build_timer_id = nil
      @lod_background_build_pending = false
      @lod_background_tasks = nil
      @lod_background_context = nil
      @max_display_point_ramp = nil
    end

    def build_octree_async?
      if defined?(PointCloudImporter::Config)
        PointCloudImporter::Config.build_octree_async?
      else
        !!@settings[:build_octree_async]
      end
    end

    def warn_background_build_failure(error)
      message = "[PointCloudImporter] Background cache build failed: #{error.message}"
      backtrace = error.backtrace ? error.backtrace.join("\n") : nil
      warn([message, backtrace].compact.join("\n"))
    rescue StandardError
      nil
    end

    def build_octree_for_points(points)
      return nil unless points && !points.empty?

      Octree.new(points,
                 max_points_per_node: Octree::DEFAULT_MAX_POINTS_PER_NODE,
                 max_depth: Octree::DEFAULT_MAX_DEPTH)
    end

    def draw_cache(view, cache, weight, style)
      return unless cache

      points = cache[:points]
      return unless points && !points.empty?

      alpha_weight = weight.to_f
      return if alpha_weight <= 0.0

      colors = cache[:colors]

      visible_batches(view, cache) do |start_index, end_index|
        points_slice = points.slice(start_index..end_index)
        draw_options = { size: @point_size }
        draw_options[:style] = style if style
        if colors
          colors_slice = colors.slice(start_index..end_index)
          draw_options[:colors] = if alpha_weight >= 0.999
                                    colors_slice
                                  else
                                    faded_colors(colors_slice, alpha_weight)
                                  end
        elsif alpha_weight < 0.999
          draw_options[:color] = blended_monochrome(alpha_weight)
        end

        view.draw_points(points_slice, **draw_options)
      end
    end

    def faded_colors(colors, weight)
      return colors if weight >= 0.999
      return [] unless colors

      colors.map do |color|
        next unless color

        faded = Sketchup::Color.new(color)
        faded.alpha = (color.alpha * weight).round.clamp(0, 255)
        faded
      end
    end

    def blended_monochrome(weight)
      alpha = (255 * weight).round.clamp(0, 255)
      Sketchup::Color.new(255, 255, 255, alpha)
    end

    def update_active_lod_level(view)
      caches = @lod_caches
      return unless caches && !caches.empty?

      camera = view&.camera
      return unless camera
      return unless camera.respond_to?(:eye)

      eye = camera.eye
      return unless eye

      bbox = bounding_box
      center = bbox&.center
      return unless center

      radius = bounding_radius
      return if radius <= 0.0

      distance = eye.distance(center)
      ratio = distance / radius
      ratio = 0.0 unless ratio.finite?

      target_level = determine_lod_level(ratio)
      return if target_level == @lod_current_level
      return unless caches[target_level]

      @lod_previous_level = @lod_current_level
      @lod_current_level = target_level
      @lod_transition_start = Time.now
    end

    def determine_lod_level(ratio)
      return LOD_LEVELS.first unless ratio.is_a?(Numeric) && ratio.finite?

      current_index = if @lod_current_level
                        LOD_RULES.index { |rule| rule[:level] == @lod_current_level } || 0
                      else
                        LOD_RULES.index { |rule| ratio <= rule[:exit_ratio] } || (LOD_RULES.length - 1)
                      end

      current_rule = LOD_RULES[current_index]

      if ratio > current_rule[:exit_ratio]
        while current_index < LOD_RULES.length - 1 && ratio > LOD_RULES[current_index][:exit_ratio]
          current_index += 1
        end
      elsif ratio < current_rule[:enter_ratio]
        while current_index.positive? && ratio < LOD_RULES[current_index][:enter_ratio]
          current_index -= 1
        end
      end

      LOD_RULES[current_index][:level]
    end

    def lod_transition_progress
      return 1.0 unless @lod_previous_level && @lod_transition_start

      elapsed = Time.now - @lod_transition_start
      progress = elapsed / LOD_FADE_DURATION
      progress = 0.0 if progress.nan? || progress.negative?
      [progress, 1.0].min
    rescue StandardError
      1.0
    end

    def finalize_lod_transition!
      @lod_previous_level = nil
      @lod_transition_start = nil
    end

    def bounding_radius
      bbox = bounding_box
      return 0.0 unless bbox

      diagonal = bbox.diagonal
      return 0.0 unless diagonal

      diagonal.to_f * 0.5
    rescue StandardError
      0.0
    end

    def invalidate_display_cache!
      @display_cache_dirty = true
    end

    def compute_step(limit: nil)
      total_points = points.length
      return 1 if total_points <= 0
      return 1 if @display_density >= 0.999

      step = (1.0 / @display_density).round
      step = 1 if step < 1

      configured_limit = limit.nil? ? @max_display_points.to_i : limit.to_i
      configured_limit = 0 if configured_limit.negative?
      limit_value = configured_limit.positive? ? configured_limit : DEFAULT_DISPLAY_POINT_CAP

      if total_points > LARGE_POINT_COUNT_THRESHOLD
        limit_value = [limit_value, DEFAULT_DISPLAY_POINT_CAP].min
      end

      estimated = (total_points / step)
      if estimated > limit_value && limit_value.positive?
        step = (total_points.to_f / limit_value).ceil
      elsif limit_value <= 0
        step = (total_points.to_f / DEFAULT_DISPLAY_POINT_CAP).ceil
      end

      step.clamp(1, total_points)
    end

    def clear_octree!
      mutex = (@octree_mutex ||= Mutex.new)
      mutex.synchronize do
        if @octree
          @octree.clear if @octree.respond_to?(:clear)
          @octree = nil
        end
      end
    end

    def reset_frustum_cache!
      @cached_frustum = nil
      @cached_camera_hash = nil
    end

    def batches(points)
      return enum_for(:batches, points) unless block_given?
      return unless points

      start_index = 0
      total_points = points.length

      while start_index < total_points
        end_index = [start_index + DRAW_BATCH_SIZE - 1, total_points - 1].min
        yield(start_index, end_index)
        start_index = end_index + 1
      end
    end

    def visible_batches(view, cache)
      return enum_for(:visible_batches, view, cache) unless block_given?
      return unless cache

      points = cache[:points]
      return unless points

      total_points = points.length
      return if total_points.zero?

      octree = ensure_octree_for_cache(cache)
      frustum = ensure_frustum(view)

      indices = nil
      if octree && frustum
        indices = octree.query_frustum(frustum)
        @last_octree_query_stats = octree.last_query_stats
      else
        indices = (0...total_points).to_a
        @last_octree_query_stats = nil
      end

      indices = indices ? indices.dup : []
      indices = filter_indices_for_density(indices)
      indices = enforce_max_display_limit(indices)

      flush_index_batch(indices) do |start_index, end_index|
        yield(start_index, end_index)
      end
    end

    def ensure_octree_for_cache(cache)
      return nil unless cache

      points = cache[:points]
      return nil unless points && !points.empty?

      mutex = (@octree_mutex ||= Mutex.new)
      octree = nil
      needs_build = false

      mutex.synchronize do
        metadata = cache[:octree_metadata] ||= {}
        octree = cache[:octree]

        if octree && should_rebuild_octree?(metadata)
          octree.clear if octree.respond_to?(:clear)
          cache[:octree] = nil
          octree = nil
        end

        needs_build = octree.nil?
      end

      if needs_build
        built_octree = build_octree_for_points(points)

        mutex.synchronize do
          cache[:octree] ||= built_octree
          metadata = cache[:octree_metadata] ||= {}
          octree = cache[:octree]
          metadata[:max_display_points_snapshot] = @max_display_points

          if cache.equal?(current_lod_cache)
            @octree = octree
            @octree_metadata = metadata
          end
        end
      else
        mutex.synchronize do
          metadata = cache[:octree_metadata] ||= {}
          octree = cache[:octree]
          metadata[:max_display_points_snapshot] = @max_display_points

          if cache.equal?(current_lod_cache)
            @octree = octree
            @octree_metadata = metadata
          end
        end
      end

      build_octree_if_needed(octree)
      octree
    end

    def build_octree_if_needed(octree)
      return unless octree

      octree.build unless octree.built?
      octree
    end

    def current_lod_cache
      return nil unless @lod_caches

      @lod_caches[@lod_current_level]
    end

    def should_rebuild_octree?(metadata)
      snapshot = metadata[:max_display_points_snapshot]
      return false unless snapshot

      snapshot = snapshot.to_f
      return false unless snapshot.positive?

      delta = (@max_display_points.to_f - snapshot).abs
      return false if delta.zero?

      (delta / snapshot) > 0.5
    end

    def ensure_frustum(view)
      hash = camera_state_hash(view)
      if @cached_frustum && @cached_camera_hash == hash
        return @cached_frustum
      end

      frustum = Frustum.from_view(view)
      @cached_frustum = frustum
      @cached_camera_hash = hash
      frustum
    end

    def camera_state_hash(view)
      camera = view&.camera
      return nil unless camera

      eye = camera.eye
      target = camera.target
      fov = camera.respond_to?(:fov) ? camera.fov.to_f : 0.0

      values = [eye.x, eye.y, eye.z, target.x, target.y, target.z, fov]
      values.map { |value| format('%0.3f', value.to_f) }.join(':')
    rescue StandardError
      nil
    end

    def normalize_density(value)
      density = value.to_f
      density = 0.01 if density <= 0.0
      density = 1.0 if density > 1.0
      density
    end

    def filter_indices_for_density(indices)
      return [] if indices.nil?
      return indices if indices.empty?
      return indices if @display_density >= 0.999

      step = (1.0 / @display_density).round
      step = 1 if step < 1
      subsample_indices(indices, step)
    end

    def enforce_max_display_limit(indices)
      return [] if indices.nil?

      limit = effective_display_limit
      return indices if limit <= 0 || indices.length <= limit

      step = (indices.length.to_f / limit).ceil
      step = 1 if step < 1
      subsampled = subsample_indices(indices, step)
      subsampled[0, limit] || []
    end

    def subsample_indices(indices, step)
      return [] if indices.nil?
      return indices if step <= 1

      result = []
      indices.each_with_index do |index, position|
        result << index if (position % step).zero?
      end
      result
    end

    def effective_display_limit
      limit = @max_display_points.to_i
      return limit if limit <= 0

      base_size = base_cache_point_count
      return limit if base_size <= 0

      [limit, base_size].min
    end

    def flush_index_batch(indices)
      return if indices.empty?

      indices.sort!
      range_start = indices.first
      previous_index = range_start

      indices[1..-1]&.each do |index|
        if index == previous_index + 1
          previous_index = index
          next
        end

        yield(range_start, previous_index)
        range_start = index
        previous_index = index
      end

      yield(range_start, previous_index)
      indices.clear
    end

    def compute_bounds_efficient!(points)
      if points.nil? || points.empty?
        reset_bounds_state!
        return
      end

      min_x = Float::INFINITY
      min_y = Float::INFINITY
      min_z = Float::INFINITY
      max_x = -Float::INFINITY
      max_y = -Float::INFINITY
      max_z = -Float::INFINITY

      points.each do |point|
        coords = point_coordinates(point)
        next unless coords

        x, y, z = coords

        min_x = x if x < min_x
        max_x = x if x > max_x
        min_y = y if y < min_y
        max_y = y if y > max_y
        min_z = z if z < min_z
        max_z = z if z > max_z
      end

      if min_x == Float::INFINITY
        reset_bounds_state!
        return
      end

      @bounds_min_x = min_x
      @bounds_min_y = min_y
      @bounds_min_z = min_z
      @bounds_max_x = max_x
      @bounds_max_y = max_y
      @bounds_max_z = max_z

      @bounding_box_dirty = true
    end

    def compute_bounds!
      compute_bounds_efficient!(points)
      finalize_bounds!
    end

    def ensure_bounding_box_initialized!
      rebuild_bounding_box_from_bounds_if_needed!
      @bounding_box ||= Geom::BoundingBox.new
    end

    def build_spatial_index!
      return if @spatial_index

      build_display_cache! unless @display_points
      return unless @display_points && !@display_points.empty?
      return unless @display_point_indices && !@display_point_indices.empty?

      @spatial_index = SpatialIndex.new(@display_points, @display_point_indices)
    end

    def compute_world_tolerance(view, pixel_tolerance)
      return pixel_tolerance.to_f if view.nil?

      bbox = bounding_box
      center = bbox&.center
      return pixel_tolerance.to_f unless center

      candidate = view.pixels_to_model(pixel_tolerance.to_f, center)
      if candidate.nil? || candidate <= 0.0
        diagonal = bbox.diagonal.to_f
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
      bbox = bounding_box
      return [Geom::Point3d.new(0, 0, 0), Geom::Point3d.new(0, 0, 0)] unless bbox

      min_point = bbox.min
      max_point = bbox.max
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

    class << self
      # Guard: class-level stub to avoid accidental calls in class context
      def octree_debug_enabled?; false; end

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

      def register_finalizer(instance, name:, points:, colors:, intensities: nil)
        info = {
          name: name,
          disposed: false,
          points_ref: points ? WeakRef.new(points) : nil,
          colors_ref: colors ? WeakRef.new(colors) : nil,
          intensities_ref: intensities ? WeakRef.new(intensities) : nil
        }

        ObjectSpace.define_finalizer(instance, finalizer_proc(info))
        info
      end

      module Finalizer
        module_function

        def safe_cleanup(_object_id, info)
          return unless info.is_a?(Hash)
          return unless defined?(PointCloudImporter::PointCloud)

          point_cloud_class = PointCloudImporter::PointCloud
          return unless point_cloud_class.respond_to?(:lingering_references, true)

          name = info[:name]
          disposed = info[:disposed]
          lingering = point_cloud_class.lingering_references(info)

          if !disposed
            detail = lingering.empty? ? 'без остаточных ссылок' : "остатки: #{lingering.join(', ')}"
            Kernel.warn("[PointCloudImporter] PointCloud '#{name}' уничтожен GC без вызова dispose! (#{detail})")
          elsif lingering.any?
            Kernel.warn("[PointCloudImporter] PointCloud '#{name}' уничтожен GC с оставшимися ссылками: #{lingering.join(', ')}")
          end
        rescue StandardError => e
          Kernel.warn("[PointCloudImporter] Ошибка финализатора для облака '#{name}': #{e.message}")
          nil
        end
      end

      def finalizer_proc(info)
        proc do |object_id|
          Finalizer.safe_cleanup(object_id, info)
        end
      end

      def lingering_references(info)
        [].tap do |refs|
          refs << 'points' if weakref_alive?(info[:points_ref])
          refs << 'colors' if weakref_alive?(info[:colors_ref])
          refs << 'intensities' if weakref_alive?(info[:intensities_ref])
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
