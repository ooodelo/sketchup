# encoding: utf-8
# frozen_string_literal: true

require 'weakref'
require 'thread'

require_relative 'clock'
require_relative 'fmt'
require_relative 'numbers'
require_relative 'settings'
require_relative 'spatial_index'
require_relative 'octree'
require_relative 'chunked_array'
require_relative 'inference_sampler'
require_relative 'logger'
require_relative 'threading'
require_relative 'main_thread_queue'
require_relative 'progress_estimator'
require_relative 'telemetry_logger'
require_relative 'ui_status'

module PointCloudImporter
  # Data structure representing a point cloud and display preferences.
  class PointCloud
    attr_reader :name, :points, :colors, :intensities
    attr_accessor :visible
    attr_reader :point_size, :point_style, :inference_sample_indices, :inference_mode
    attr_reader :color_mode, :color_gradient, :last_visible_point_count, :last_sampling_duration

    def rename!(new_name)
      candidate = new_name.to_s
      candidate = 'Point Cloud' if candidate.strip.empty?
      @name = candidate
      self
    end

    def manager
      return nil unless defined?(@manager_ref)

      reference = @manager_ref
      return nil unless reference

      reference.__getobj__
    rescue WeakRef::RefError
      @manager_ref = nil
      nil
    end

    def manager=(manager)
      @manager_ref = manager ? WeakRef.new(manager) : nil
    rescue StandardError
      @manager_ref = manager
    end

    def importing?
      !!@importing
    end

    def begin_import!
      @importing = true
      @pending_import_tasks = {}
      mark_post_import_task!(:display_cache)
      self
    end

    def complete_import!(success: true, scheduler: MainThreadScheduler.instance)
      @importing = false
      pending = @pending_import_tasks || {}
      @pending_import_tasks = {}

      return self unless success

      tasks = %i[display_cache color_refresh spatial_index].select { |task| pending[task] }
      return self if tasks.empty?

      if scheduler
        kickoff_display_pipeline!(pending_tasks: tasks, scheduler: scheduler)
      else
        prepare_initial_frame!(manager&.view)
        tasks.each { |task| run_post_import_task(task) }
        release_initial_frame_constraints!
        finalize_kickoff_background!
      end

      self
    end

    def abort_import!
      @importing = false
      @pending_import_tasks = {}
      cancel_background_lod_build!
      cancel_color_rebuild!
      self
    end

    POINT_STYLE_CANDIDATES = {
      square: %i[DRAW_POINTS_SQUARES DRAW_POINTS_SQUARE DRAW_POINTS_OPEN_SQUARE],
      round: %i[DRAW_POINTS_ROUND DRAW_POINTS_OPEN_CIRCLE],
      plus: %i[DRAW_POINTS_PLUS DRAW_POINTS_CROSS]
    }.each_with_object({}) do |(style, candidates), memo|
      memo[style] = candidates.freeze
    end.freeze

    LOD_LEVELS = [1.0, 0.5, 0.25, 0.1, 0.05].freeze

    LOD_RULES = [
      { level: 1.0, enter_ratio: 0.0, exit_ratio: 1.5 },
      { level: 0.5, enter_ratio: 1.2, exit_ratio: 2.5 },
      { level: 0.25, enter_ratio: 2.0, exit_ratio: 3.5 },
      { level: 0.1, enter_ratio: 3.0, exit_ratio: 5.0 },
      { level: 0.05, enter_ratio: 4.5, exit_ratio: Float::INFINITY }
    ].map(&:freeze).freeze

    LOD_FADE_DURATION = 0.35

    BACKGROUND_TIMER_INTERVAL = 0.015
    BACKGROUND_STEP_BUDGET = 0.02
    BACKGROUND_SAMPLE_MIN_SIZE = 50_000
    BACKGROUND_MAX_ITERATIONS = 10_000
    BACKGROUND_MAX_DURATION = 120.0

    KICKOFF_INITIAL_DENSITY = 0.3
    KICKOFF_STAGE_DELAY = 0.05
    KICKOFF_RELAX_DELAY = 0.5

    LARGE_POINT_COUNT_THRESHOLD = 3_000_000
    DEFAULT_DISPLAY_POINT_CAP = 1_200_000

    DEFAULT_SINGLE_COLOR_HEX = '#ffffff'
    PACKED_COLOR_MASK = 0x00ffffff
    DEFAULT_PACKED_COLOR = 0x00ffffff

    COLOR_DEBOUNCE_INTERVAL = 0.15
    COLOR_REBUILD_BATCH = 100_000
    COLOR_REBUILD_INITIAL_BATCH = 75_000
    HEAVY_COLOR_MODES = %i[rgb_xyz].freeze
    GRADIENT_LUT_SIZE = 256

    FRUSTUM_POSITION_QUANTUM = 0.1
    FRUSTUM_FOV_QUANTUM = 0.25

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
      ],
      jet: [
        [0.0, [0, 0, 131]],
        [0.35, [0, 255, 255]],
        [0.5, [255, 255, 0]],
        [0.75, [255, 0, 0]],
        [1.0, [128, 0, 0]]
      ],
      grayscale: [
        [0.0, [0, 0, 0]],
        [1.0, [255, 255, 255]]
      ],
      magma: [
        [0.0, [0, 0, 4]],
        [0.25, [78, 18, 123]],
        [0.5, [187, 55, 84]],
        [0.75, [249, 142, 8]],
        [1.0, [251, 252, 191]]
      ]
    }.each_with_object({}) do |(key, stops), memo|
      memo[key] = stops.map do |stop|
        position, color = stop
        color_array = Array(color).dup.freeze
        [position, color_array].freeze
      end.freeze
    end.freeze

    class << self
      def pack_rgb(r, g, b)
        return nil if r.nil? || g.nil? || b.nil?

        r_value = r.to_i & 0xff
        g_value = g.to_i & 0xff
        b_value = b.to_i & 0xff
        (r_value << 16) | (g_value << 8) | b_value
      rescue StandardError
        nil
      end

      def unpack_rgb(value)
        return nil if value.nil?

        masked = value.to_i & PACKED_COLOR_MASK
        [
          (masked >> 16) & 0xff,
          (masked >> 8) & 0xff,
          masked & 0xff
        ]
      rescue StandardError
        nil
      end

      def gradient_lut_for(key)
        cache = gradient_lut_cache
        effective_key = key || COLOR_GRADIENTS.keys.first
        effective_key ||= COLOR_GRADIENTS.keys.first

        cache.fetch(effective_key) do
          cache[effective_key] = build_gradient_lut(effective_key)
        end
      end

      private

      def gradient_lut_cache
        @gradient_lut_cache ||= {}
      end

      def build_gradient_lut(key)
        stops = COLOR_GRADIENTS[key] || COLOR_GRADIENTS[:viridis]
        return [].freeze unless stops && !stops.empty?

        lut = Array.new(GRADIENT_LUT_SIZE) do |index|
          t = if GRADIENT_LUT_SIZE <= 1
                0.0
              else
                index.to_f / (GRADIENT_LUT_SIZE - 1)
              end

          lower = stops.first
          upper = stops.last

          stops.each do |stop|
            if t <= stop[0]
              upper = stop
              break
            end
            lower = stop
          end

          lower_color = Array(lower && lower[1])
          upper_color = Array(upper && upper[1])

          if upper == lower
            pack_rgb(lower_color[0], lower_color[1], lower_color[2]) || DEFAULT_PACKED_COLOR
          else
            range = upper[0] - lower[0]
            weight = range.abs < Float::EPSILON ? 0.0 : (t - lower[0]) / range
            r = interpolate_component(lower_color[0], upper_color[0], weight)
            g = interpolate_component(lower_color[1], upper_color[1], weight)
            b = interpolate_component(lower_color[2], upper_color[2], weight)
            pack_rgb(r, g, b) || DEFAULT_PACKED_COLOR
          end
        end

        lut.freeze
      end

      def interpolate_component(min_component, max_component, weight)
        min_value = min_component.to_f
        max_value = max_component.to_f
        return min_value.to_i & 0xff if weight.nil?

        (min_value + ((max_value - min_value) * weight.to_f)).round.clamp(0, 255)
      rescue StandardError
        min_component.to_i & 0xff
      end
    end

    def initialize(name:, points: nil, colors: nil, intensities: nil, metadata: {}, chunk_capacity: nil,
                   settings_snapshot: nil)
      @settings_snapshot = settings_snapshot
      @settings_version = settings_snapshot&.version
      @settings = settings_snapshot || Settings.instance
      setting_chunk_capacity = @settings[:chunk_capacity].to_i
      setting_chunk_capacity = ChunkedArray::DEFAULT_CHUNK_CAPACITY if setting_chunk_capacity <= 0
      provided_chunk_capacity = chunk_capacity && chunk_capacity.to_i
      resolved_chunk_capacity = if provided_chunk_capacity && provided_chunk_capacity.positive?
                                  provided_chunk_capacity
                                else
                                  setting_chunk_capacity
                                end

      @name = name
      @points = ChunkedArray.new(resolved_chunk_capacity)
      @colors = colors ? ChunkedArray.new(resolved_chunk_capacity) : nil
      @intensities = intensities ? ChunkedArray.new(resolved_chunk_capacity) : nil
      @metadata = (metadata || {}).dup
      @visible = true
      @point_size = @settings[:point_size]
      @point_style = ensure_valid_style(@settings[:point_style])
      persist_style!(@point_style) if @point_style != @settings[:point_style]
      @display_density = @settings[:density]
      @max_display_points = @settings[:max_display_points]
      @color_mode = normalize_color_mode(@settings[:color_mode])
      @color_gradient = normalize_color_gradient(@settings[:color_gradient])
      @single_color = parse_color_setting(@settings[:single_color])
      @packed_single_color = nil
      @display_points = nil
      @display_colors = nil
      @display_index_lookup = nil
      @display_point_indices = nil
      @spatial_index = nil
      @spatial_index_generation = nil
      @spatial_index_metadata = nil
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
      @lod_background_task = nil
      @lod_cache_generation = 0
      @lod_background_steps = nil
      @lod_background_context = nil
      @lod_background_generation = 0
      @lod_background_suspension = nil
      @max_display_point_ramp = nil
      @render_density_override = nil
      @render_max_points_override = nil
      @intensity_min = nil
      @intensity_max = nil
      @random_seed = deterministic_seed_for(name)
      @importing = false
      @pending_import_tasks = {}
      @bounding_box = Geom::BoundingBox.new
      @bounding_box_dirty = false
      @bounds_min_x = nil
      @bounds_min_y = nil
      @bounds_min_z = nil
      @bounds_max_x = nil
      @bounds_max_y = nil
      @bounds_max_z = nil
      @bbox_cache = {}
      @cache_dirty = false
      @render_cache_preparation_pending = false
      @manager_ref = nil
      @packed_color_cache = {}
      @active_color_buffer = nil
      @active_color_buffer_key = nil
      @color_rebuild_pending = false
      @color_rebuild_generation = 0
      @color_rebuild_task = nil
      @color_rebuild_debounce_task = nil
      @color_rebuild_state = nil
      @normalized_height_cache = nil
      @normalized_intensity_cache = nil
      @scalar_cache_generation = 0
      @color_generation_token = 0
      @active_color_request_token = nil
      @scalar_cache_signature = nil
      @gradient_lut = nil
      @gradient_lut_key = nil
      @color_geometry_generation = 0
      @cached_color_bounds = nil
      @cached_color_bounds_generation = nil
      @random_color_cache = nil
      @random_color_cache_generation = nil
      @color_progress_estimator = ProgressEstimator.new
      @color_metrics = nil
      @last_color_rebuild_summary = nil
      @last_visible_point_count = 0
      @last_sampling_duration = nil
      append_points!(points, colors, intensities) if points && !points.empty?

      @inference_sample_indices = nil
      @inference_mode = nil
      @user_max_display_points = @max_display_points
      mark_display_cache_dirty! if points && !points.empty?

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
      @bbox_cache = nil
      @octree_mutex = nil
      @manager_ref = nil
      cancel_color_rebuild!
      mark_finalizer_disposed!
    end

    def clear_cache!
      cancel_background_lod_build!
      cancel_color_rebuild!
      invalidate_color_geometry!
      @color_progress_estimator&.reset
      @cache_dirty = true
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
      @render_density_override = nil
      @render_max_points_override = nil
      @packed_color_cache = {}
      @active_color_buffer = nil
      @active_color_buffer_key = nil
      @normalized_height_cache = nil
      @normalized_intensity_cache = nil
      @scalar_cache_generation = 0
      @scalar_cache_signature = nil
      @lod_background_task = nil
      @lod_background_suspension = nil
    end

    def visible?
      @visible
    end

    def bounding_box
      rebuild_bounding_box_from_bounds_if_needed!
      @bounding_box
    end

    def finalize_bounds!
      @points.trim_last_chunk!
      @colors&.trim_last_chunk!
      @intensities&.trim_last_chunk!
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

    def apply_render_constraints!(density: nil, max_display_points: nil)
      @render_density_override = density ? normalize_density(density) : @render_density_override
      @render_max_points_override = max_display_points ? max_display_points.to_i : @render_max_points_override
      self
    end

    def clear_render_constraints!
      @render_density_override = nil
      @render_max_points_override = nil
      self
    end

    def render_density
      override = @render_density_override
      override ? normalize_density(override) : @display_density
    end

    def render_max_display_points
      override = @render_max_points_override
      override.nil? ? @max_display_points.to_i : override.to_i
    end

    def color_rebuild_pending?
      !!@color_rebuild_pending
    end

    def consume_last_color_rebuild_summary
      summary = @last_color_rebuild_summary
      @last_color_rebuild_summary = nil
      summary
    end

    def current_lod_level
      @lod_current_level || LOD_LEVELS.first
    end

    def lod0_point_count
      cache = @lod_caches ? @lod_caches[LOD_LEVELS.first] : nil
      points = cache ? cache[:points] : nil
      points ? points.length : 0
    end

    def background_build_pending?
      !!@lod_background_build_pending
    end

    def color_mode=(mode)
      normalized = normalize_color_mode(mode)
      normalized = :height if normalized == :intensity && !has_intensity?
      return if normalized == @color_mode || normalized.nil?

      @color_mode = normalized
      settings = Settings.instance
      settings[:color_mode] = @color_mode.to_s
      refresh_color_buffers!(debounce: true)
    end

    def color_gradient=(gradient)
      normalized = normalize_color_gradient(gradient)
      return if normalized.nil? || normalized == @color_gradient

      @color_gradient = normalized
      settings = Settings.instance
      settings[:color_gradient] = @color_gradient.to_s

      @gradient_lut = nil
      @gradient_lut_key = nil

      return unless color_mode_requires_gradient?

      refresh_color_buffers!(debounce: true)
    end

    def single_color=(value)
      color = parse_color_setting(value)
      return unless color
      return if colors_equal?(@single_color, color)

      @single_color = color
      @packed_single_color = nil
      settings = Settings.instance
      settings[:single_color] = color_to_hex(@single_color)

      return unless color_mode_requires_single_color?

      refresh_color_buffers!(debounce: true)
    end

    def single_color
      @single_color ||= parse_color_setting(DEFAULT_SINGLE_COLOR_HEX)
    end

    def packed_single_color
      color = single_color
      @packed_single_color ||= begin
        packed = pack_color_value(color)
        packed.nil? ? DEFAULT_PACKED_COLOR : packed
      end
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

    def max_display_points
      @max_display_points
    end

    def prepare_render_cache!
      return self if defer_import_task(:display_cache)

      ensure_display_caches!
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

    def draw(view, draw_context = nil)
      Threading.guard(:ui, message: 'PointCloud#draw')
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
        draw_cache(view, previous_cache, 1.0 - progress, style, draw_context) if previous_cache
        draw_cache(view, current_cache, progress, style, draw_context)
      else
        draw_cache(view, current_cache, 1.0, style, draw_context)
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
        ensure_display_caches!
        return if @display_points.nil? || @display_points.empty?

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
          colors[index] = pack_color_value(new_color)
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
        normalized_colors = Array(colors_array).map { |color| pack_color_value(color) }
        @colors ||= ChunkedArray.new(@points.chunk_capacity)
        @colors.clear
        @colors.append_chunk(normalized_colors)
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

      clear_color_cache!

      compute_bounds_efficient!(points_array)
      finalize_bounds!
      invalidate_display_cache!
      self
    end

    def append_points!(points_chunk, colors_chunk = nil, intensities_chunk = nil, bounds: nil, intensity_range: nil)
      return if points_chunk.nil? || points_chunk.empty?

      @points.append_batch!(points_chunk)

      if colors_chunk && !colors_chunk.empty?
        @colors ||= ChunkedArray.new(@points.chunk_capacity)
        if colors_chunk.all? { |color| color.is_a?(Integer) }
          @colors.append_batch!(colors_chunk)
        else
          normalized_colors = colors_chunk.map { |color| pack_color_value(color) }
          @colors.append_batch!(normalized_colors)
        end
      end

      if intensities_chunk && !intensities_chunk.empty?
        @intensities ||= ChunkedArray.new(@points.chunk_capacity)
        @intensities.append_batch!(intensities_chunk)
      end

      clear_color_cache!

      bounds_applied = bounds && apply_bounds_aggregate(bounds)
      update_bounds_with_chunk(points_chunk) unless bounds_applied

      intensity_applied = intensity_range && apply_intensity_range_aggregate(intensity_range)
      if !intensity_applied && intensities_chunk && !intensities_chunk.empty?
        update_intensity_range!(intensities_chunk)
      end

      invalidate_color_geometry!

      mark_display_cache_dirty!
    end

    def update_metadata!(metadata)
      @metadata = (metadata || {}).dup
    end

  private

    def kickoff_display_pipeline!(pending_tasks:, scheduler:, view: nil)
      scheduler ||= MainThreadScheduler.instance
      tasks = Array(pending_tasks).dup

      stages = []
      kickoff_view = view || manager&.view

      stages << { action: -> { prepare_initial_frame!(kickoff_view) }, delay: KICKOFF_STAGE_DELAY }

      tasks.each do |task|
        stages << { action: -> { run_post_import_task(task) }, delay: KICKOFF_STAGE_DELAY }
        stages << { action: -> { release_initial_frame_constraints! }, delay: KICKOFF_RELAX_DELAY } if task == :display_cache
      end

      stages << { action: -> { finalize_kickoff_background! }, delay: KICKOFF_STAGE_DELAY }

      scheduler.schedule(name: 'point-cloud-kickoff', priority: 110) do |context|
        entry = stages.shift
        return :done unless entry

        begin
          entry[:action]&.call
        rescue StandardError => e
          Logger.debug { "Kickoff stage failed: #{e.class}: #{e.message}" }
        end

        if stages.empty?
          :done
        else
          delay = entry[:delay].to_f
          context.reschedule_in = delay.positive? ? delay : KICKOFF_STAGE_DELAY
          :pending
        end
      end
    end

    def prepare_initial_frame!(_view)
      ensure_display_caches!
      limit = initial_display_limit
      density = render_density
      density = KICKOFF_INITIAL_DENSITY if density > KICKOFF_INITIAL_DENSITY
      density = 0.01 if density <= 0.0
      payload_limit = limit && limit.positive? ? limit : nil
      apply_render_constraints!(density: density, max_display_points: payload_limit)
      throttled_view_invalidate
    end

    def release_initial_frame_constraints!
      clear_render_constraints!
      throttled_view_invalidate
    end

    def finalize_kickoff_background!
      resume_background_lod_build!
      throttled_view_invalidate
    end

    def initial_display_limit
      cache = @lod_caches ? @lod_caches[LOD_LEVELS.first] : nil
      cache_size = cache && cache[:points] ? cache[:points].length : points&.length.to_i
      limit = resolved_startup_cap
      limit = cache_size if limit <= 0
      if cache_size.positive? && limit.positive?
        [limit, cache_size].min
      else
        limit
      end
    end

    def mark_post_import_task!(task)
      @pending_import_tasks ||= {}
      @pending_import_tasks[task] = true
    end

    def defer_import_task(task)
      return false unless importing?

      mark_post_import_task!(task)
      true
    end

    def run_post_import_task(task)
      case task
      when :display_cache
        build_display_cache!
      when :color_refresh
        refresh_color_buffers!
      when :spatial_index
        build_spatial_index!
      end
    rescue StandardError => e
      Logger.debug { "Post-import task #{task} завершился с ошибкой: #{e.class}: #{e.message}" }
      nil
    end

    def mark_display_cache_dirty!
      return if @cache_dirty

      invalidate_display_cache!
    end

    def rebuild_bounding_box_from_bounds_if_needed!
      return unless @bounding_box_dirty || @bounding_box.nil?

      rebuild_bounding_box_from_bounds!
    end

    def reset_bounds_state!
      invalidate_color_geometry!
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

    def apply_bounds_aggregate(bounds)
      return false unless bounds.is_a?(Hash)

      min_x = coerce_numeric(bounds[:min_x])
      min_y = coerce_numeric(bounds[:min_y])
      min_z = coerce_numeric(bounds[:min_z])
      max_x = coerce_numeric(bounds[:max_x])
      max_y = coerce_numeric(bounds[:max_y])
      max_z = coerce_numeric(bounds[:max_z])

      return false if [min_x, min_y, min_z, max_x, max_y, max_z].any?(&:nil?)

      if @bounds_min_x.nil?
        @bounds_min_x = min_x
        @bounds_min_y = min_y
        @bounds_min_z = min_z
        @bounds_max_x = max_x
        @bounds_max_y = max_y
        @bounds_max_z = max_z
      else
        @bounds_min_x = [@bounds_min_x, min_x].min
        @bounds_min_y = [@bounds_min_y, min_y].min
        @bounds_min_z = [@bounds_min_z, min_z].min
        @bounds_max_x = [@bounds_max_x, max_x].max
        @bounds_max_y = [@bounds_max_y, max_y].max
        @bounds_max_z = [@bounds_max_z, max_z].max
      end

      @bounding_box_dirty = true
      @bbox_cache = nil
      true
    end

    def apply_intensity_range_aggregate(range)
      return false unless range.is_a?(Hash)

      min_value = coerce_numeric(range[:min])
      max_value = coerce_numeric(range[:max])

      if min_value && max_value && min_value > max_value
        min_value, max_value = max_value, min_value
      end

      applied = false

      if min_value
        @intensity_min = if @intensity_min.nil?
                           min_value
                         else
                           [@intensity_min, min_value].min
                         end
        applied = true
      end

      if max_value
        @intensity_max = if @intensity_max.nil?
                           max_value
                         else
                           [@intensity_max, max_value].max
                         end
        applied = true
      end

      applied
    end

    def coerce_numeric(value)
      return nil unless value.respond_to?(:to_f)

      numeric = value.to_f
      return nil unless numeric.finite?

      numeric
    rescue StandardError
      nil
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
      @bbox_cache = nil
    end

    def invalidate_color_geometry!
      @color_geometry_generation = @color_geometry_generation.to_i + 1
      @cached_color_bounds = nil
      @cached_color_bounds_generation = nil
      @random_color_cache = nil
      @random_color_cache_generation = nil
      @normalized_height_cache = nil
      @normalized_intensity_cache = nil
      @scalar_cache_generation = 0
      @scalar_cache_signature = nil
      @bbox_cache = nil
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
      elsif stored.is_a?(Integer)
        self.class.unpack_rgb(stored)
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

    def pack_color_components(components)
      return nil unless components

      self.class.pack_rgb(components[0], components[1], components[2])
    rescue StandardError
      nil
    end

    def pack_color_value(value)
      return nil if value.nil?

      if value.is_a?(Integer)
        value & PACKED_COLOR_MASK
      else
        components = fetch_color_components(value)
        components ? pack_color_components(components) : nil
      end
    rescue StandardError
      nil
    end

    def scalar_cache_signature
      [
        @color_geometry_generation,
        @points ? @points.length : 0,
        @intensities ? @intensities.length : 0,
        @bounds_min_x, @bounds_min_y, @bounds_min_z,
        @bounds_max_x, @bounds_max_y, @bounds_max_z,
        @intensity_min, @intensity_max
      ]
    end

    def ensure_scalar_cache_context!
      signature = scalar_cache_signature
      generation = @color_geometry_generation
      total_points = @points ? @points.length : 0

      return if @scalar_cache_signature == signature &&
                @scalar_cache_generation == generation &&
                @normalized_height_cache &&
                @normalized_height_cache.length == total_points &&
                (!@normalized_intensity_cache.nil? || !has_intensity?)

      build_scalar_cache!(generation, signature)
    end

    def build_scalar_cache!(generation, signature)
      total = @points ? @points.length : 0
      if total <= 0
        @normalized_height_cache = []
        @normalized_intensity_cache = []
      else
        heights = Array.new(total, 0.5)
        intensity_cache = has_intensity? ? Array.new(total, 0.5) : nil
        points = @points
        intensities = @intensities if intensity_cache

        total.times do |index|
          point = points[index]
          height = point ? normalize_height(point) : 0.5
          heights[index] = height

          next unless intensity_cache

          raw_intensity = intensities ? intensities[index] : nil
          intensity_cache[index] = if raw_intensity.nil?
                                     height
                                   else
                                     normalize_intensity(raw_intensity)
                                   end
        end

        @normalized_height_cache = heights
        @normalized_intensity_cache = intensity_cache || heights
      end

      @scalar_cache_generation = generation
      @scalar_cache_signature = signature
    rescue StandardError
      @normalized_height_cache = nil
      @normalized_intensity_cache = nil
      @scalar_cache_generation = generation
      @scalar_cache_signature = signature
    end

    def normalized_height_for(index)
      ensure_scalar_cache_context!
      cache = @normalized_height_cache
      return 0.5 unless cache && index

      value = cache[index.to_i]
      value.nil? ? 0.5 : value
    rescue StandardError
      0.5
    end

    def normalized_intensity_for(index)
      ensure_scalar_cache_context!
      cache = @normalized_intensity_cache
      if cache && index
        value = cache[index.to_i]
        return value unless value.nil?
      end

      normalized_height_for(index)
    rescue StandardError
      0.5
    end

    def batch_vertices_limit
      limit = @settings[:batch_vertices_limit].to_i
      return Settings::DEFAULTS[:batch_vertices_limit] if limit <= 0

      limit
    end
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

    def clear_color_cache!
      @packed_color_cache = {}
      @active_color_buffer = nil
      @active_color_buffer_key = nil
      @gradient_lut = nil
      @gradient_lut_key = nil
    end

    def cancel_color_rebuild!
      @color_rebuild_generation = @color_rebuild_generation.to_i + 1
      if @color_rebuild_task
        @color_rebuild_task.cancel
        @color_rebuild_task = nil
      end
      if @color_rebuild_debounce_task
        @color_rebuild_debounce_task.cancel
        @color_rebuild_debounce_task = nil
      end
      @color_rebuild_pending = false
      @color_rebuild_state = nil
      @active_color_request_token = nil
      resume_background_lod_build!
    end

    def next_color_request_token
      token = (@color_generation_token = @color_generation_token.to_i + 1)
      @active_color_request_token = token
      token
    rescue StandardError
      @active_color_request_token = nil
      @color_generation_token = 0
      0
    end

    def stale_color_request?(generation, token)
      return true if @cache_dirty
      return true unless generation == @color_rebuild_generation

      active = @active_color_request_token
      return true if active.nil?

      token != active
    rescue StandardError
      true
    end

    def current_color_cache_key
      gradient_key = color_mode_requires_gradient? ? @color_gradient : nil
      single_color_key = color_mode_requires_single_color? ? color_to_hex(@single_color) : nil
      intensity_key = @color_mode == :intensity ? [@intensity_min, @intensity_max] : nil
      bounds_key = if %i[height rgb_xyz].include?(@color_mode)
                     [@bounds_min_x, @bounds_min_y, @bounds_min_z,
                      @bounds_max_x, @bounds_max_y, @bounds_max_z]
                   end

      [@color_mode, gradient_key, single_color_key, intensity_key, bounds_key]
    end

    def packed_color_cache_for_current_mode
      return nil unless color_buffer_enabled?

      total = @points ? @points.length : 0
      return [] if total <= 0

      key = current_color_cache_key
      cache = (@packed_color_cache ||= {})
      buffer = cache[key]
      if buffer.nil? || buffer.length != total
        buffer = Array.new(total)
        cache[key] = buffer
      end

      @active_color_buffer = buffer
      @active_color_buffer_key = key
      buffer
    rescue StandardError
      cache.delete(key) if cache && key
      @active_color_buffer = nil
      @active_color_buffer_key = nil
      nil
    end

    def packed_display_color_for_index(index)
      case @color_mode
      when :original
        value = @colors ? @colors[index] : nil
        value = pack_color_value(value) unless value.is_a?(Integer)
        value.nil? ? DEFAULT_PACKED_COLOR : (value & PACKED_COLOR_MASK)
      when :height
        packed_gradient_color(normalized_height_for(index))
      when :intensity
        packed_gradient_color(normalized_intensity_for(index))
      when :single
        packed_single_color
      when :random
        packed_random_color(index)
      when :rgb_xyz
        point = @points[index]
        point ? packed_rgb_from_xyz(point) : DEFAULT_PACKED_COLOR
      else
        value = @colors ? @colors[index] : nil
        value = pack_color_value(value) unless value.is_a?(Integer)
        value.nil? ? DEFAULT_PACKED_COLOR : (value & PACKED_COLOR_MASK)
      end
    rescue StandardError
      DEFAULT_PACKED_COLOR
    end

    def display_color_for_index(original_index)
      return nil if original_index.nil?

      buffer = packed_color_cache_for_current_mode
      if buffer && original_index < buffer.length
        packed = buffer[original_index]
        return color_from_storage(packed) if packed
      end

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
      components = gradient_components(value)
      Sketchup::Color.new(components[0], components[1], components[2])
    end

    def gradient_components(value)
      packed = packed_gradient_sample(value)
      fetch_color_components(packed) || [255, 255, 255]
    end

    def gradient_lut
      key = @color_gradient || COLOR_GRADIENTS.keys.first
      key ||= COLOR_GRADIENTS.keys.first
      if @gradient_lut && @gradient_lut_key == key
        @gradient_lut
      else
        lut = self.class.send(:gradient_lut_for, key)
        @gradient_lut_key = key
        @gradient_lut = lut
      end
    end

    def packed_gradient_color(value)
      packed_gradient_sample(value)
    end

    def packed_gradient_sample(value)
      lut = gradient_lut
      return DEFAULT_PACKED_COLOR unless lut && !lut.empty?

      t = clamp01(value.to_f)
      scaled = t * (lut.length - 1)
      lower_index = scaled.floor
      lower_index = 0 if lower_index.negative?
      upper_index = [lower_index + 1, lut.length - 1].min
      fraction = scaled - lower_index

      lower = lut[lower_index] || DEFAULT_PACKED_COLOR
      upper = lut[upper_index] || lower

      return lower if upper_index == lower_index || fraction.abs < Float::EPSILON

      lr = (lower >> 16) & 0xff
      lg = (lower >> 8) & 0xff
      lb = lower & 0xff
      ur = (upper >> 16) & 0xff
      ug = (upper >> 8) & 0xff
      ub = upper & 0xff

      r = (lr + ((ur - lr) * fraction)).round.clamp(0, 255)
      g = (lg + ((ug - lg) * fraction)).round.clamp(0, 255)
      b = (lb + ((ub - lb) * fraction)).round.clamp(0, 255)

      self.class.pack_rgb(r, g, b) || DEFAULT_PACKED_COLOR
    rescue StandardError
      DEFAULT_PACKED_COLOR
    end

    def clamp01(value)
      Numbers.clamp(value.to_f, 0.0, 1.0)
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

    def heavy_color_mode?(mode = @color_mode)
      HEAVY_COLOR_MODES.include?(mode)
    end

    def color_economy_threshold
      value = @settings[:color_economy_threshold]
      value = value.to_i
      return value if value.positive?

      fallback = Settings::DEFAULTS[:color_economy_threshold]
      fallback ? fallback.to_i : 0
    rescue StandardError
      Settings::DEFAULTS[:color_economy_threshold]
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
      color_from_storage(packed_random_color(index))
    end

    def ensure_random_color_cache
      total = @points ? @points.length : 0
      return nil if total <= 0

      generation = @color_geometry_generation
      cache_generation = @random_color_cache_generation
      cache = @random_color_cache

      if cache.nil? || cache.length != total || cache_generation != generation
        cache = build_random_color_table(total, generation)
        @random_color_cache = cache
        @random_color_cache_generation = generation
      end

      cache
    rescue StandardError
      @random_color_cache = nil
      nil
    end

    def build_random_color_table(total, generation)
      seed = (@random_seed || 0) ^ generation.to_i
      state = seed & 0xffffffff
      Array.new(total) do
        state = ((state * 1_664_525) + 1_013_904_223) & 0xffffffff
        r = (((state >> 16) & 0xff) / 2.0 + 64).round.clamp(0, 255)
        g = (((state >> 8) & 0xff) / 2.0 + 64).round.clamp(0, 255)
        b = ((state & 0xff) / 2.0 + 64).round.clamp(0, 255)
        pack_color_components([r, g, b]) || DEFAULT_PACKED_COLOR
      end
    rescue StandardError
      Array.new(total, DEFAULT_PACKED_COLOR)
    end

    def packed_random_color(index)
      cache = ensure_random_color_cache
      return DEFAULT_PACKED_COLOR unless cache

      cached = cache[index.to_i]
      cached.nil? ? DEFAULT_PACKED_COLOR : cached
    rescue StandardError
      DEFAULT_PACKED_COLOR
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
      return 0.5 if range.abs < Float::EPSILON

      clamp01((value.to_f - min) / range)
    rescue StandardError
      0.0
    end

    def bounding_box_extents
      cache = (@bbox_cache ||= {})
      generation = @color_geometry_generation
      return cache if cache[:version] == generation && cache.key?(:min)

      min_x = @bounds_min_x
      min_y = @bounds_min_y
      min_z = @bounds_min_z
      max_x = @bounds_max_x
      max_y = @bounds_max_y
      max_z = @bounds_max_z

      if [min_x, min_y, min_z, max_x, max_y, max_z].any?(&:nil?)
        cache[:min] = nil
        cache[:max] = nil
        cache[:range] = nil
      else
        min = [min_x.to_f, min_y.to_f, min_z.to_f].freeze
        max = [max_x.to_f, max_y.to_f, max_z.to_f].freeze
        range = [max[0] - min[0], max[1] - min[1], max[2] - min[2]].freeze
        cache[:min] = min
        cache[:max] = max
        cache[:range] = range
      end

      cache[:version] = generation
      cache
    rescue StandardError
      @bbox_cache = { version: generation }
      @bbox_cache
    end

    def color_bounds
      generation = @color_geometry_generation
      cache_generation = @cached_color_bounds_generation
      cached = @cached_color_bounds

      if cache_generation != generation || cached.nil?
        extents = bounding_box_extents
        min = extents[:min]
        max = extents[:max]
        range = extents[:range]

        if min.nil? || max.nil? || range.nil?
          cached = nil
        else
          cached = {
            min_x: min[0],
            min_y: min[1],
            min_z: min[2],
            max_x: max[0],
            max_y: max[1],
            max_z: max[2],
            range_x: range[0],
            range_y: range[1],
            range_z: range[2]
          }
        end

        @cached_color_bounds = cached
        @cached_color_bounds_generation = generation
      end

      cached
    rescue StandardError
      @cached_color_bounds = nil
      @cached_color_bounds_generation = generation
      nil
    end

    def normalize_height(point)
      bounds = color_bounds
      return 0.5 unless bounds

      z = point_coordinate(point, :z)
      return 0.5 if z.nil?

      min_z = bounds[:min_z]
      range = bounds[:range_z]
      return 0.5 if !range || range.abs < Float::EPSILON

      clamp01((z.to_f - min_z) / range)
    rescue StandardError
      0.5
    end

    def rgb_from_xyz(point)
      color_from_storage(packed_rgb_from_xyz(point))
    end

    def packed_rgb_from_xyz(point)
      coords = point_coordinates(point)
      return DEFAULT_PACKED_COLOR unless coords

      bounds = color_bounds
      return DEFAULT_PACKED_COLOR unless bounds

      x = normalize_component(coords[0], bounds[:min_x], bounds[:max_x])
      y = normalize_component(coords[1], bounds[:min_y], bounds[:max_y])
      z = normalize_component(coords[2], bounds[:min_z], bounds[:max_z])

      pack_color_components([
        (x * 255).round.clamp(0, 255),
        (y * 255).round.clamp(0, 255),
        (z * 255).round.clamp(0, 255)
      ]) || DEFAULT_PACKED_COLOR
    rescue StandardError
      DEFAULT_PACKED_COLOR
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
      return if defer_import_task(:display_cache)

      finalize_bounds!
      @cache_dirty = false
      @spatial_index = nil
      cancel_background_lod_build!
      clear_octree!
      reset_frustum_cache!
      @last_octree_query_stats = nil
      @octree_metadata = nil
      @lod_caches = {}
      @lod_cache_generation = (@lod_cache_generation || 0) + 1
      invalidate_color_geometry!

      return unless points && !points.empty?

      target_limit = @user_max_display_points.to_i
      target_limit = @settings[:max_display_points].to_i if target_limit <= 0
      target_limit = DEFAULT_DISPLAY_POINT_CAP if target_limit <= 0

      available_points = points.length
      if available_points.positive? && target_limit > available_points
        target_limit = available_points
      end

      @user_max_display_points = target_limit

      startup_limit = resolved_startup_cap
      startup_cap = [target_limit, startup_limit].min
      startup_cap = available_points if startup_cap <= 0 && available_points.positive?
      startup_cap = startup_limit if startup_cap <= 0

      base_cache = build_base_cache(limit: startup_cap)
      @lod_caches[LOD_LEVELS.first] = base_cache if base_cache

      assign_primary_cache(base_cache)
      @lod_current_level ||= LOD_LEVELS.first
      @lod_previous_level = nil
      @lod_transition_start = nil

      base_size = base_cache && base_cache[:points] ? base_cache[:points].length : 0
      @max_display_points = base_size
      @max_display_point_ramp = nil

      start_background_lod_build(target_limit)
    end

    def ensure_display_caches!
      return if defer_import_task(:display_cache)

      build_display_cache! if @cache_dirty || @lod_caches.nil?

      base_cache = @lod_caches&.fetch(LOD_LEVELS.first, nil)
      if @display_points.nil? && base_cache
        assign_primary_cache(base_cache)
      end

      if base_cache && (!build_octree_async? || base_cache[:octree])
        ensure_octree_for_cache(base_cache)
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

    def refresh_color_buffers!(debounce: false)
      return if defer_import_task(:color_refresh)

      return build_display_cache! if @cache_dirty || @lod_caches.nil?
      return unless points && !points.empty?

      cache = @lod_caches[LOD_LEVELS.first]
      unless cache && cache[:point_indices]
        build_display_cache!
        return
      end

      unless color_buffer_enabled?
        clear_color_cache!
        @lod_caches.each_value do |lod_cache|
          next unless lod_cache

          lod_cache[:colors] = nil
        end
        assign_primary_cache(@lod_caches[@lod_current_level || LOD_LEVELS.first])
        return
      end

      _packed_buffer = packed_color_cache_for_current_mode
      ensure_lod_color_arrays!

      cancel_color_rebuild!
      generation = @color_rebuild_generation
      request_token = next_color_request_token
      @color_rebuild_pending = true
      suspend_background_lod_build_for_color!(generation)

      scheduler = MainThreadScheduler.instance
      launcher = lambda do
        @color_rebuild_task = scheduler.schedule(name: 'color-rebuild',
                                                 priority: 200,
                                                 cancel_if: -> { stale_color_request?(generation, request_token) }) do |context|
          process_color_rebuild(context, generation)
        end
      end

      if debounce
        @color_rebuild_debounce_task = scheduler.schedule(name: 'color-rebuild-debounce',
                                                          priority: 50,
                                                          delay: COLOR_DEBOUNCE_INTERVAL,
                                                          cancel_if: -> { stale_color_request?(generation, request_token) }) do
          if stale_color_request?(generation, request_token)
            :done
          else
            launcher.call
            :done
          end
        end
      else
        launcher.call
      end

      assign_primary_cache(@lod_caches[@lod_current_level || LOD_LEVELS.first])
    end

    def suspend_background_lod_build_for_color!(generation)
      background_generation = @lod_background_generation
      return unless background_generation && background_generation.positive?

      @lod_background_suspension = {
        background_generation: background_generation,
        color_generation: generation
      }
    end

    def background_build_suspended?
      suspension = @lod_background_suspension
      return false unless suspension

      suspension[:background_generation] == @lod_background_generation &&
        suspension[:color_generation] == @color_rebuild_generation
    rescue StandardError
      false
    end

    def resume_background_lod_build!(generation = nil)
      suspension = @lod_background_suspension
      return unless suspension

      if generation && suspension[:color_generation] != generation
        return
      end

      @lod_background_suspension = nil
    end

    def initialize_color_rebuild_metrics(total:, primary_total:, generation:, economy:, color_mode:)
      estimator = (@color_progress_estimator ||= ProgressEstimator.new)
      estimator.reset(total_vertices: total)
      @color_metrics = {
        generation: generation,
        started_at: safe_monotonic_time,
        total: total,
        primary_total: primary_total,
        processed: 0,
        primary_processed: 0,
        primary_completed_at: nil,
        peak_memory: current_memory_usage,
        last_rate: nil,
        economy: economy ? economy.dup : nil,
        color_mode: color_mode
      }
    end

    def update_color_rebuild_metrics(state, primary_in_batch)
      metrics = @color_metrics
      return unless metrics

      metrics[:processed] = state[:processed]
      state_economy = state[:economy]
      metrics[:economy] ||= state_economy ? state_economy.dup : nil

      if primary_in_batch.positive?
        metrics[:primary_processed] = state[:primary_processed]
        if metrics[:primary_completed_at].nil? &&
           metrics[:primary_total].to_i.positive? &&
           metrics[:primary_processed] >= metrics[:primary_total]
          metrics[:primary_completed_at] = safe_monotonic_time
          resume_background_lod_build!(state[:generation])
        end
      end

      usage = current_memory_usage
      if usage && usage.respond_to?(:to_i)
        usage = usage.to_i
        previous = metrics[:peak_memory]
        metrics[:peak_memory] = previous ? [previous, usage].max : usage
      end

      emit_color_status_if_needed(state)
    end

    def emit_color_status_if_needed(state, force: false)
      metrics = @color_metrics
      return unless metrics

      estimator = (@color_progress_estimator ||= ProgressEstimator.new)
      estimator.update(processed_vertices: state[:processed])
      fraction = if state[:total].positive?
                   state[:processed].to_f / state[:total]
                 else
                   nil
                 end

      message = build_color_status_message(metrics, fraction)
      now = Clock.monotonic

      should_emit = force || estimator.poll(message: message,
                                           fraction: fraction,
                                           now: now,
                                           respect_message_change: false)
      return unless should_emit

      UIStatus.set(message)
    end

    def build_color_status_message(metrics, fraction)
      processed = metrics[:processed].to_i
      total = metrics[:total].to_i
      started_at = metrics[:started_at]
      elapsed = started_at ? (safe_monotonic_time - started_at) : nil
      rate = if elapsed && elapsed.positive?
               processed / elapsed
             else
               0.0
             end
      metrics[:last_rate] = rate

      percent = fraction ? (fraction * 100.0) : 0.0

      base_message = format('Перекраска облака: %<percent>.1f%% (%<processed>s/%<total>s, %<rate>s)',
                            percent: percent,
                            processed: format_point_count(processed),
                            total: format_point_count(total),
                            rate: format_rate(rate))

      peak_memory = metrics[:peak_memory]
      memory_note = if peak_memory && peak_memory.to_i.positive?
                      "пик: #{Fmt.bytes(peak_memory)}"
                    end

      [base_message, memory_note].compact.join(' • ')
    rescue StandardError
      'Перекраска облака'
    end

    def current_memory_usage
      stats = GC.stat
      stats[:heap_live_slots] || stats[:heap_used] || stats[:total_allocated_objects]
    rescue StandardError
      nil
    end

    def format_point_count(value)
      Fmt.n(value, round_small: false)
    rescue StandardError
      '0'
    end

    def format_rate(value)
      Fmt.n(value, suffix: ' тчк/с')
    rescue StandardError
      '0 тчк/с'
    end

    def finalize_color_rebuild_metrics(success)
      metrics = @color_metrics
      return unless metrics

      finished_at = safe_monotonic_time
      metrics[:processed] = metrics[:processed].to_i
      metrics[:finished_at] = finished_at

      summary = compile_color_rebuild_summary(metrics, success)
      emit_color_rebuild_logs(summary)
      @last_color_rebuild_summary = summary

      @color_metrics = nil
    end

    def compile_color_rebuild_summary(metrics, success)
      started_at = metrics[:started_at] || metrics[:finished_at]
      finished_at = metrics[:finished_at] || safe_monotonic_time
      total_duration = finished_at - started_at
      primary_duration = if metrics[:primary_completed_at]
                           metrics[:primary_completed_at] - started_at
                         else
                           0.0
                         end
      total_duration = 0.0 if total_duration&.negative?
      primary_duration = 0.0 if primary_duration&.negative?

      processed = metrics[:processed].to_i
      total = metrics[:total].to_i
      rate = if total_duration && total_duration.positive?
               processed / total_duration
             else
               0.0
             end

      {
        cloud: name.to_s,
        generation: metrics[:generation].to_i,
        success: !!success,
        processed: processed,
        total: total,
        primary_total: metrics[:primary_total].to_i,
        duration: total_duration || 0.0,
        primary_duration: primary_duration || 0.0,
        rate: rate,
        peak_memory: metrics[:peak_memory].to_i,
        color_mode: metrics[:color_mode],
        economy: metrics[:economy]
      }
    rescue StandardError
      nil
    end

    def emit_color_rebuild_logs(summary)
      return unless summary

      Logger.debug do
        format('color_rebuild;%<cloud>s;%<generation>d;%<success>s;%<processed>d;%<total>d;%<primary_total>d;%<duration>.3f;%<primary_duration>.3f;%<rate>.1f;%<peak_memory>d;%<economy>s',
               cloud: summary[:cloud],
               generation: summary[:generation],
               success: summary[:success] ? 1 : 0,
               processed: summary[:processed],
               total: summary[:total],
               primary_total: summary[:primary_total],
               duration: summary[:duration] || 0.0,
               primary_duration: summary[:primary_duration] || 0.0,
               rate: summary[:rate] || 0.0,
               peak_memory: summary[:peak_memory],
               economy: summary.dig(:economy, :enabled) ? 'economy' : 'full')
      end

      TelemetryLogger.instance.log_color_rebuild(summary)
    rescue StandardError => e
      Logger.debug { "Color rebuild telemetry failed: #{e.message}" }
    end

    def compute_color_economy(total_points:, visible_points:, color_mode:)
      threshold = color_economy_threshold.to_i
      visible = visible_points.to_i
      total = total_points.to_i

      enabled = heavy_color_mode?(color_mode) && threshold.positive? && total > threshold && visible > threshold
      limit = enabled ? [threshold, visible].min : visible
      limit = visible if limit <= 0
      step = visible.positive? ? (visible.to_f / [limit, 1].max).ceil : 1
      step = 1 if step <= 0

      {
        enabled: enabled,
        threshold: threshold,
        visible: visible,
        limit: limit,
        step: step
      }
    rescue StandardError
      {
        enabled: false,
        threshold: color_economy_threshold.to_i,
        visible: visible_points.to_i,
        limit: visible_points.to_i,
        step: 1
      }
    end

    def downsample_primary_indices(indices, economy)
      return [] unless indices

      collection = indices.dup
      return collection unless economy && economy[:enabled]

      visible = collection.length
      limit = economy[:limit].to_i
      limit = visible if limit <= 0 || limit > visible
      return collection if limit >= visible || limit <= 0

      stride = economy[:step].to_i
      stride = (visible.to_f / limit).ceil if stride <= 0
      stride = 1 if stride <= 0

      sampled = []
      collection.each_with_index do |index, position|
        next unless (position % stride).zero?

        sampled << index
        break if sampled.length >= limit
      end

      sampled = collection.first(limit) if sampled.empty?
      economy[:step] = stride
      economy[:limit] = limit
      sampled
    rescue StandardError
      indices ? indices.dup : []
    end

    def build_base_cache(limit:)
      return nil unless points && !points.empty?

      limit = limit.to_i
      limit = DEFAULT_DISPLAY_POINT_CAP if limit <= 0

      step = compute_step(limit: limit)
      base_points = []
      base_colors = color_buffer_enabled? ? [] : nil
      base_index_lookup = {}
      base_point_indices = []
      packed_colors = base_colors ? packed_color_cache_for_current_mode : nil

      index = 0
      iterator = points.respond_to?(:each_with_yield) ? :each_with_yield : :each
      points.public_send(iterator) do |raw_point|
        if (index % step).zero?
          point = point3d_from(raw_point)
          if point
            base_points << point
            if base_colors
              packed = packed_colors && index < packed_colors.length ? packed_colors[index] : nil
              packed ||= packed_display_color_for_index(index)
              base_colors << packed
            end
            base_index_lookup[index] = base_points.length - 1
            base_point_indices << index
            break if limit.positive? && base_points.length >= limit
          end
        end
        index += 1
      end

      create_cache(LOD_LEVELS.first,
                   base_points,
                   base_colors,
                   base_index_lookup,
                   base_point_indices)
    end

    def ensure_lod_color_arrays!
      return unless @lod_caches

      @lod_caches.each_value do |cache|
        next unless cache

        indices = cache[:point_indices]
        next unless indices

        colors = cache[:colors]
        if colors.nil? || colors.length != indices.length
          cache[:colors] = Array.new(indices.length)
        end
      end
    end

    def ensure_color_rebuild_state(generation)
      state = @color_rebuild_state
      return state if state && state[:generation] == generation

      @color_rebuild_state = build_color_rebuild_state(generation)
    end

    def build_color_rebuild_state(generation)
      total = @points ? @points.length : 0
      return nil if total <= 0

      base_cache = current_lod_cache || @lod_caches&.fetch(LOD_LEVELS.first, nil)
      primary_candidates = base_cache ? Array(base_cache[:point_indices]) : []
      economy = compute_color_economy(total_points: total,
                                      visible_points: primary_candidates.length,
                                      color_mode: @color_mode)

      sampled_primary = downsample_primary_indices(primary_candidates, economy)
      primary_enum = sampled_primary.empty? ? nil : sampled_primary.each

      primary_lookup = {}
      sampled_primary.each { |index| primary_lookup[index] = true }

      primary_total = sampled_primary.length
      economy[:primary_total] = primary_total if economy

      secondary_enum = Enumerator.new do |yielder|
        (0...total).each do |index|
          next if primary_lookup[index]

          yielder << index
        end
      end

      initialize_color_rebuild_metrics(total: total,
                                       primary_total: primary_total,
                                       generation: generation,
                                       economy: economy,
                                       color_mode: @color_mode)
      resume_background_lod_build!(generation) if primary_total.zero?

      {
        generation: generation,
        primary_enum: primary_enum,
        secondary_enum: secondary_enum,
        processed: 0,
        total: total,
        primary_total: primary_total,
        primary_processed: 0,
        last_source: nil,
        economy: economy
      }
    end

    def fetch_next_color_index(state)
      return nil unless state

      loop do
        enumerator = state[:primary_enum]
        if enumerator
          begin
            value = enumerator.next
            state[:last_source] = :primary
            return value
          rescue StopIteration
            state[:primary_enum] = nil
          end
          next
        end

        enumerator = state[:secondary_enum]
        return nil unless enumerator

        begin
          value = enumerator.next
          state[:last_source] = :secondary
          return value
        rescue StopIteration
          state[:secondary_enum] = nil
          return nil
        end
      end
    end

    def color_rebuild_state_complete?(state)
      return true unless state

      state[:primary_enum].nil? && state[:secondary_enum].nil?
    end

    def process_color_rebuild(context, generation)
      return :done unless generation == @color_rebuild_generation

      buffer = @active_color_buffer || packed_color_cache_for_current_mode
      return :done unless buffer

      state = ensure_color_rebuild_state(generation)
      return :done unless state

      batch_limit = state[:processed].zero? ? COLOR_REBUILD_INITIAL_BATCH : COLOR_REBUILD_BATCH
      processed = 0
      primary_in_batch = 0
      updated = []
      deadline = context&.deadline

      loop do
        break if processed >= batch_limit
        break if deadline && safe_monotonic_time >= deadline

        index = fetch_next_color_index(state)
        break unless index

        source = state[:last_source]
        if source == :primary
          primary_in_batch += 1
          state[:primary_processed] = state[:primary_processed].to_i + 1
        end

        buffer[index] = packed_display_color_for_index(index)
        updated << index
        processed += 1
        state[:processed] += 1
      end

      update_color_rebuild_metrics(state, primary_in_batch)

      unless updated.empty?
        apply_packed_colors_to_caches(updated, buffer)
        throttled_view_invalidate
      end

      if color_rebuild_state_complete?(state)
        finalize_color_rebuild(success: true)
        :done
      else
        context.reschedule_in = 0.0 if context
        :pending
      end
    rescue StandardError => e
      Logger.debug { "Color rebuild failed: #{e.class}: #{e.message}" }
      finalize_color_rebuild(success: false)
      :done
    end

    def apply_packed_colors_to_caches(indices, buffer)
      return unless indices && buffer
      return unless @lod_caches

      @lod_caches.each_value do |cache|
        next unless cache

        colors = cache[:colors]
        lookup = cache[:index_lookup]
        next unless colors && lookup

        indices.each do |original_index|
          position = lookup[original_index]
          next if position.nil?

          colors[position] = buffer[original_index]
        end
      end
    end

    def finalize_color_rebuild(success: true)
      state = @color_rebuild_state
      emit_color_status_if_needed(state, force: true) if state
      finalize_color_rebuild_metrics(success)
      resume_background_lod_build!(state ? state[:generation] : nil)
      @color_rebuild_pending = false
      @color_rebuild_task = nil
      @color_rebuild_state = nil
      @active_color_request_token = nil
      throttled_view_invalidate
    end

    def create_cache(level, points, colors, index_lookup, point_indices)
      cache_points = points ? points : []
      cache_colors = colors.nil? ? nil : colors
      cache_index_lookup = index_lookup ? index_lookup : {}
      cache_point_indices = point_indices ? point_indices : []

      {
        level: level,
        points: cache_points,
        colors: cache_colors,
        index_lookup: cache_index_lookup,
        point_indices: cache_point_indices,
        octree: nil,
        octree_metadata: nil
      }
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

    def start_background_lod_build(target_limit)
      caches = @lod_caches
      base_cache = caches ? caches[LOD_LEVELS.first] : nil
      base_size = base_cache && base_cache[:points] ? base_cache[:points].length : 0

      target_limit = target_limit.to_i

      @lod_background_generation = @lod_background_generation.to_i + 1
      background_generation = @lod_background_generation

      context = {
        generation: @lod_cache_generation,
        background_generation: background_generation,
        target_limit: target_limit,
        needs_ramp: target_limit.positive? && target_limit > base_size,
        started_at: Process.clock_gettime(Process::CLOCK_MONOTONIC),
        iterations: 0
      }

      @lod_background_context = context
      @max_display_point_ramp = nil
      @lod_background_suspension = nil

      steps = build_background_steps(context, target_limit, base_size, background_generation)

      if steps.empty? && !context[:needs_ramp]
        finalize_background_build(context)
        return
      end

      @lod_background_steps = steps
      @lod_background_build_pending = true

      scheduler = MainThreadScheduler.instance
      @lod_background_task = scheduler.schedule(name: 'lod-background-build',
                                                priority: 120,
                                                delay: 0.0) do |context|
        continue = process_background_steps
        if continue
          context.reschedule_in = BACKGROUND_TIMER_INTERVAL
          :pending
        else
          :done
        end
      end
    end

    def build_background_steps(context, target_limit, base_size, background_generation)
      steps = []

      if target_limit.positive? && base_size < target_limit
        steps << -> { expand_lod0_to!(target_limit, background_generation) }
      end

      LOD_LEVELS[1..-1].each do |level|
        steps << -> { rebuild_lod!(level, background_generation) }
      end

      generation = context[:generation]
      total_points = determine_background_point_target(target_limit, base_size)

      if total_points.positive?
        octree_sample_sizes = sampling_sizes_for_total(total_points,
                                                       min_size: BACKGROUND_SAMPLE_MIN_SIZE,
                                                       max_size: max_background_sample_size)
        octree_sample_sizes.each_with_index do |sample_size, sample_index|
          steps << lambda { build_octree_sampled(generation, background_generation, sample_index, sample_size) }
        end

        if octree_sample_sizes.empty? || octree_sample_sizes.last < total_points
          steps << -> { build_octree_full(generation, background_generation) }
        end

        spatial_sample_sizes = sampling_sizes_for_total(total_points,
                                                        min_size: BACKGROUND_SAMPLE_MIN_SIZE,
                                                        max_size: max_background_sample_size)
        spatial_sample_sizes.each_with_index do |sample_size, sample_index|
          steps << lambda { build_spatial_index_sampled(generation, background_generation, sample_index, sample_size) }
        end

        if spatial_sample_sizes.empty? || spatial_sample_sizes.last < total_points
          steps << -> { build_spatial_index_full(generation, background_generation) }
        end
      end

      steps
    end

    def determine_background_point_target(target_limit, base_size)
      available_points = points ? points.length.to_i : 0
      capped_target = target_limit.positive? ? [target_limit, available_points].min : available_points
      [capped_target, base_size, available_points].max
    end

    def background_context_active?(context, background_generation = nil)
      return false unless context
      return false unless context[:generation] == @lod_cache_generation

      current_background = @lod_background_generation
      context_generation = context[:background_generation]
      expected_generation = background_generation || context_generation

      return false if current_background.nil?
      return false if context_generation.nil?
      return false unless expected_generation == current_background
      return false unless context_generation == current_background

      true
    end

    def process_background_steps
      if @cache_dirty
        cancel_background_lod_build!
        return false
      end

      context = @lod_background_context
      unless background_context_active?(context)
        finalize_background_build(context)
        return false
      end

      if background_build_suspended?
        context[:suspended] = true if context
        return true
      end

      iterations = context[:iterations].to_i + 1
      context[:iterations] = iterations
      if iterations > BACKGROUND_MAX_ITERATIONS
        finalize_background_build(context)
        return false
      end

      started_at = context[:started_at]
      if started_at
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
        if elapsed.positive? && elapsed > BACKGROUND_MAX_DURATION
          finalize_background_build(context)
          return false
        end
      end

      steps = @lod_background_steps

      if steps && !steps.empty?
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + BACKGROUND_STEP_BUDGET
        while steps.any?
          step = steps.shift
          step.call if step
          break if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        end
      end

      maybe_start_max_display_point_ramp(context)
      ramp_active = continue_max_display_point_ramp

      if (@lod_background_steps.nil? || @lod_background_steps.empty?) && !ramp_active
        finalize_background_build(context)
        throttled_view_invalidate
        return false
      end

      throttled_view_invalidate
      true
    rescue StandardError => error
      warn_background_build_failure(error)
      finalize_background_build(context)
      false
    end

    def expand_lod0_to!(target_limit, background_generation = nil)
      context = @lod_background_context
      return unless background_context_active?(context, background_generation)

      caches = @lod_caches
      return unless caches

      base_cache = caches[LOD_LEVELS.first]
      return unless base_cache

      target_limit = target_limit.to_i
      return if target_limit <= 0

      current_size = base_cache[:points] ? base_cache[:points].length : 0
      return if current_size >= target_limit

      new_cache = build_base_cache(limit: target_limit)
      return unless new_cache

      atomic_swap_lod!(LOD_LEVELS.first, new_cache)
      @max_display_points = [@max_display_points.to_i, target_limit].max
    end

    def rebuild_lod!(level, background_generation = nil)
      context = @lod_background_context
      return unless background_context_active?(context, background_generation)

      caches = @lod_caches
      return unless caches

      base_cache = caches[LOD_LEVELS.first]
      return unless base_cache

      new_cache = create_downsampled_cache(base_cache, level)
      atomic_swap_lod!(level, new_cache) if new_cache
    end

    def atomic_swap_lod!(level, cache)
      return unless cache

      caches = (@lod_caches ||= {})
      caches[level] = cache

      assign_primary_cache(cache) if level == LOD_LEVELS.first

      throttled_view_invalidate
    end

    def build_octree_sampled(generation, background_generation, sample_index = 0, sample_size = nil)
      return unless generation == @lod_cache_generation
      return unless background_generation == @lod_background_generation

      context = @lod_background_context
      return unless background_context_active?(context, background_generation)

      caches = @lod_caches
      return unless caches

      base_cache = caches[LOD_LEVELS.first]
      return unless base_cache

      points = base_cache[:points]
      return unless points && !points.empty?

      total_points = points.length
      effective_sample_size = sample_size_for_index(total_points,
                                                   sample_index,
                                                   min_size: BACKGROUND_SAMPLE_MIN_SIZE,
                                                   max_size: max_background_sample_size)

      limit = sample_size || effective_sample_size
      sampled_points, = sample_cache_points(base_cache, limit)
      return if sampled_points.nil? || sampled_points.empty?

      built_octree = build_octree_for_points(sampled_points)
      return unless built_octree

      return unless background_context_active?(@lod_background_context, background_generation)

      metadata = {
        sampled: true,
        sample_size: sampled_points.length,
        total_points: total_points,
        sample_index: sample_index,
        max_display_points_snapshot: @max_display_points
      }

      mutex = (@octree_mutex ||= Mutex.new)
      mutex.synchronize do
        return unless generation == @lod_cache_generation
        return unless background_generation == @lod_background_generation

        base_cache[:octree] = built_octree
        base_cache[:octree_metadata] = metadata

        current_cache = caches[LOD_LEVELS.first]
        if base_cache.equal?(current_cache)
          @octree = built_octree
          @octree_metadata = metadata
        end
      end
    end

    def build_octree_full(generation, background_generation)
      return unless generation == @lod_cache_generation
      return unless background_generation == @lod_background_generation

      context = @lod_background_context
      return unless background_context_active?(context, background_generation)

      caches = @lod_caches
      return unless caches

      base_cache = caches[LOD_LEVELS.first]
      return unless base_cache

      points = base_cache[:points]
      return unless points && !points.empty?

      built_octree = build_octree_for_points(points)
      return unless built_octree

      return unless background_context_active?(@lod_background_context, background_generation)

      metadata = {
        sampled: false,
        sample_size: points.length,
        total_points: points.length,
        max_display_points_snapshot: @max_display_points
      }

      mutex = (@octree_mutex ||= Mutex.new)
      mutex.synchronize do
        return unless generation == @lod_cache_generation
        return unless background_generation == @lod_background_generation

        base_cache[:octree] = built_octree
        base_cache[:octree_metadata] = metadata

        current_cache = caches[LOD_LEVELS.first]
        if base_cache.equal?(current_cache)
          @octree = built_octree
          @octree_metadata = metadata
        end
      end
    end

    def build_spatial_index_sampled(generation, background_generation, sample_index = 0, sample_size = nil)
      return unless generation == @lod_cache_generation
      return unless background_generation == @lod_background_generation

      context = @lod_background_context
      return unless background_context_active?(context, background_generation)

      caches = @lod_caches
      return unless caches

      base_cache = caches[LOD_LEVELS.first]
      return unless base_cache

      points = base_cache[:points]
      return unless points && !points.empty?

      total_points = points.length
      effective_sample_size = sample_size_for_index(total_points,
                                                   sample_index,
                                                   min_size: BACKGROUND_SAMPLE_MIN_SIZE,
                                                   max_size: max_background_sample_size)

      limit = sample_size || effective_sample_size
      sampled_points, sampled_indices = sample_cache_points(base_cache, limit)
      return if sampled_points.nil? || sampled_points.empty?
      return if sampled_indices.nil? || sampled_indices.empty?

      spatial_index = SpatialIndex.new(sampled_points, sampled_indices)
      return unless spatial_index

      return unless background_context_active?(@lod_background_context, background_generation)

      metadata = {
        sampled: true,
        sample_size: sampled_points.length,
        total_points: total_points,
        sample_index: sample_index
      }

      if generation == @lod_cache_generation && background_generation == @lod_background_generation
        @spatial_index = spatial_index
        @spatial_index_generation = generation
        @spatial_index_metadata = metadata
      end
    end

    def build_spatial_index_full(generation, background_generation)
      return unless generation == @lod_cache_generation
      return unless background_generation == @lod_background_generation

      context = @lod_background_context
      return unless background_context_active?(context, background_generation)

      caches = @lod_caches
      return unless caches

      base_cache = caches[LOD_LEVELS.first]
      return unless base_cache

      spatial_index = build_spatial_index_for_cache(base_cache)
      return unless spatial_index

      return unless background_context_active?(@lod_background_context, background_generation)

      metadata = {
        sampled: false,
        sample_size: base_cache[:points]&.length || 0,
        total_points: base_cache[:points]&.length || 0
      }

      if generation == @lod_cache_generation && background_generation == @lod_background_generation
        @spatial_index = spatial_index
        @spatial_index_generation = generation
        @spatial_index_metadata = metadata
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

    def sampling_sizes_for_total(total_points, min_size:, max_size:)
      total_points = total_points.to_i
      return [] if total_points <= 0

      min_size = [min_size.to_i, 1].max
      max_size = [max_size.to_i, min_size].max

      sizes = []

      first_size = [min_size, total_points].min
      sizes << first_size if first_size.positive?

      return sizes if total_points <= first_size

      second_size = [max_size, total_points].min
      sizes << second_size if second_size > first_size

      sizes
    end

    def sample_size_for_index(total_points, sample_index, min_size:, max_size:)
      sizes = sampling_sizes_for_total(total_points,
                                       min_size: min_size,
                                       max_size: max_size)
      sizes[sample_index.to_i] || total_points.to_i
    end

    def resolved_startup_cap
      value = @settings[:startup_cap].to_i
      value = Settings::DEFAULTS[:startup_cap] if value <= 0
      value
    end

    def background_invalidate_interval
      interval_ms = @settings[:invalidate_interval_ms].to_i
      interval_ms = Settings::DEFAULTS[:invalidate_interval_ms] if interval_ms <= 0
      interval_ms.to_f / 1000.0
    end

    def max_background_sample_size
      value = @settings[:max_points_sampled].to_i
      value = Settings::DEFAULTS[:max_points_sampled] if value <= 0
      value
    end

    def throttled_view_invalidate
      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      last = (@_last_invalidate_at ||= 0.0)
      return if (now - last) < background_invalidate_interval

      @_last_invalidate_at = now
      invalidate_active_view
    rescue StandardError
      nil
    end

    def maybe_start_max_display_point_ramp(context)
      return unless context
      return unless context[:needs_ramp]
      return if @max_display_point_ramp

      target_limit = context[:target_limit].to_i
      return if target_limit <= 0

      current_limit = @max_display_points.to_i
      return if current_limit >= target_limit

      @max_display_point_ramp = { target: target_limit }
    end

    def continue_max_display_point_ramp
      ramp = @max_display_point_ramp
      return false unless ramp

      target_limit = ramp[:target].to_i
      return finalize_ramp if target_limit <= 0

      current_limit = @max_display_points.to_i
      if current_limit >= target_limit
        finalize_ramp
        return false
      end

      increment = compute_max_display_point_increment(target_limit, current_limit)
      increment = target_limit - current_limit if increment > (target_limit - current_limit)
      increment = 1 if increment <= 0

      new_limit = current_limit + increment
      new_limit = target_limit if new_limit > target_limit

      @max_display_points = new_limit
      throttled_view_invalidate

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
        @max_display_points = target_limit if target_limit.positive?
      end

      @max_display_point_ramp = nil
      false
    end

    def invalidate_active_view
      view = nil

      manager = self.manager
      if manager && manager.respond_to?(:view)
        view = manager.view
      end

      if (!view || !view.respond_to?(:invalidate)) && defined?(Sketchup)
        model = Sketchup.active_model
        if model && model.respond_to?(:active_view)
          view = model.active_view
        end
      end

      return unless view && view.respond_to?(:invalidate)

      view.invalidate
    rescue StandardError
      nil
    end

    def finalize_background_build(context)
      if background_context_active?(context)
        target_limit = context[:target_limit].to_i
        if target_limit.positive? && @max_display_points < target_limit
          @max_display_points = target_limit
        end

        @lod_background_build_pending = false
      end

      @lod_background_steps = nil
      @lod_background_context = nil
      @max_display_point_ramp = nil
      if @lod_background_task
        @lod_background_task.cancel
        @lod_background_task = nil
      end
      @lod_async_build_timer_id = nil
      throttled_view_invalidate
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
      if @lod_background_task
        @lod_background_task.cancel
        @lod_background_task = nil
      end

      if @lod_async_build_timer_id && defined?(UI) && UI.respond_to?(:stop_timer)
        UI.stop_timer(@lod_async_build_timer_id)
      end

      @lod_async_build_timer_id = nil
      @lod_background_build_pending = false
      @lod_background_steps = nil
      @lod_background_context = nil
      @max_display_point_ramp = nil
      @lod_background_generation = @lod_background_generation.to_i + 1
      @lod_background_suspension = nil
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
                 max_points_per_node: @settings[:octree_max_points_per_node],
                 max_depth: @settings[:octree_max_depth])
    end

    def draw_cache(view, cache, weight, style, draw_context)
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
          converted = converted_colors(colors, start_index, end_index, alpha_weight, draw_context)
          draw_options[:colors] = converted if converted
        elsif alpha_weight < 0.999
          draw_options[:color] = blended_monochrome(alpha_weight)
        end

        view.draw_points(points_slice, **draw_options)
      end
    end

    def draw_color_resources(draw_context)
      if draw_context
        pool = draw_context[:color_pool] ||= []
        buffer = draw_context[:color_buffer] ||= []
        [pool, buffer]
      else
        [[], []]
      end
    end

    def converted_colors(packed_colors, start_index, end_index, weight, draw_context)
      return [] unless packed_colors

      length = end_index - start_index + 1
      return [] if length <= 0

      pool, buffer = draw_color_resources(draw_context)
      pool_length = pool.length
      if pool_length < length
        (pool_length...length).each { pool << nil }
      end

      buffer.length = length

      alpha = weight >= 0.999 ? 255 : (255 * weight).round.clamp(0, 255)

      length.times do |offset|
        packed = packed_colors[start_index + offset]
        packed = DEFAULT_PACKED_COLOR if packed.nil?
        r = (packed >> 16) & 0xff
        g = (packed >> 8) & 0xff
        b = packed & 0xff
        color = pool[offset]
        if color
          if color.respond_to?(:set)
            color.set(r, g, b, alpha)
          else
            color.red = r if color.respond_to?(:red=)
            color.green = g if color.respond_to?(:green=)
            color.blue = b if color.respond_to?(:blue=)
            color.alpha = alpha if color.respond_to?(:alpha=)
          end
        else
          color = Sketchup::Color.new(r, g, b, alpha)
          pool[offset] = color
        end
        buffer[offset] = color
      end

      buffer
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
      @cache_dirty = true
    end

    def compute_step(limit: nil)
      total_points = points.length
      return 1 if total_points <= 0
      return 1 if @display_density >= 0.999

      step = (1.0 / @display_density).round
      step = 1 if step < 1

      configured_limit = limit.nil? ? @max_display_points.to_i : limit.to_i
      configured_limit = 0 if configured_limit.negative?
      limit = configured_limit.positive? ? configured_limit : DEFAULT_DISPLAY_POINT_CAP

      if total_points > LARGE_POINT_COUNT_THRESHOLD
        limit = [limit, DEFAULT_DISPLAY_POINT_CAP].min
      end

      estimated = (total_points / step)
      if estimated > limit && limit.positive?
        step = (total_points.to_f / limit).ceil
      elsif limit <= 0
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

      batch_size = [batch_vertices_limit, 1].max

      while start_index < total_points
        end_index = [start_index + batch_size - 1, total_points - 1].min
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

      sampling_started_at = safe_monotonic_time
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

      visible_count = indices ? indices.length : 0
      @last_visible_point_count = visible_count

      flush_index_batch(indices) do |start_index, end_index|
        yield(start_index, end_index)
      end

      @last_sampling_duration = begin
        finish = safe_monotonic_time
        sampling_started_at && finish ? (finish - sampling_started_at) : nil
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

      return octree if octree.built?

      start_time = Time.now
      octree.build
      duration = Time.now - start_time
      point_count = if octree.points.respond_to?(:length)
                      octree.points.length
                    else
                      nil
                    end
      Logger.debug do
        format(
          'Построение октодерева: %<duration>.3fs, points=%<points>s, max_per_node=%<max>d, max_depth=%<depth>d',
          duration: duration,
          points: point_count || 'n/a',
          max: octree.max_points_per_node,
          depth: octree.max_depth
        )
      end
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

      values = [
        quantize_frustum_value(eye&.x, FRUSTUM_POSITION_QUANTUM),
        quantize_frustum_value(eye&.y, FRUSTUM_POSITION_QUANTUM),
        quantize_frustum_value(eye&.z, FRUSTUM_POSITION_QUANTUM),
        quantize_frustum_value(target&.x, FRUSTUM_POSITION_QUANTUM),
        quantize_frustum_value(target&.y, FRUSTUM_POSITION_QUANTUM),
        quantize_frustum_value(target&.z, FRUSTUM_POSITION_QUANTUM),
        quantize_frustum_value(fov, FRUSTUM_FOV_QUANTUM)
      ]

      values.map { |value| format('%0.3f', value.to_f) }.join(':')
    rescue StandardError
      nil
    end

    def safe_monotonic_time
      Clock.monotonic
    rescue StandardError
      Time.now.to_f
    end

    def quantize_frustum_value(value, quantum)
      return 0.0 if value.nil?
      return 0.0 if quantum.nil? || quantum <= 0.0

      scaled = value.to_f / quantum
      (scaled.round * quantum)
    end

    def normalize_density(value)
      density = value.to_f
      Numbers.clamp(density, 0.01, 1.0)
    end

    def filter_indices_for_density(indices)
      return [] if indices.nil?
      return indices if indices.empty?

      current_density = render_density
      return indices if current_density >= 0.999

      step = (1.0 / current_density).round
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

    def effective_display_limit
      base_cache = @lod_caches ? @lod_caches[LOD_LEVELS.first] : nil
      base_size = base_cache && base_cache[:points] ? base_cache[:points].length : 0

      override_limit = @render_max_points_override

      limit = if override_limit
                override_limit.to_i
              elsif @lod_background_build_pending
        requested_limit = @user_max_display_points.to_i
        requested_limit = @settings[:max_display_points].to_i if requested_limit <= 0
        requested_limit = DEFAULT_DISPLAY_POINT_CAP if requested_limit <= 0

                if base_size <= 0
                  requested_limit
                elsif requested_limit <= 0
                  base_size
                else
                  [requested_limit, base_size].min
                end
              else
                @max_display_points.to_i
              end

      if base_size.positive?
        return base_size if limit <= 0

        [limit, base_size].min
      else
        limit
      end
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

      iterator = points.respond_to?(:each_with_yield) ? :each_with_yield : :each
      points.public_send(iterator) do |point|
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
      @bbox_cache = nil
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
      return if defer_import_task(:spatial_index)

      ensure_display_caches!
      return if @spatial_index
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

          t1 = -Float::INFINITY
          t2 = Float::INFINITY
        else
          inv = 1.0 / d
          t1 = (min_axis - o) * inv
          t2 = (max_axis - o) * inv
          t1, t2 = t2, t1 if t1 > t2
        end

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
