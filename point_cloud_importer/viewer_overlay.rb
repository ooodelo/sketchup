# encoding: utf-8
# frozen_string_literal: true

require_relative 'logger'
require_relative 'threading'

module PointCloudImporter
  # Overlay responsible for rendering active point clouds efficiently.
  class ViewerOverlay < Sketchup::Overlay
    DRAW_CHUNK = 250_000
    STARTUP_DENSITY = 0.2
    RAMP_DURATION = 1.5
    MAX_LIMIT_EPSILON = 1
    DENSITY_EPSILON = 0.01
    DIAGNOSTIC_UPDATE_INTERVAL = 0.2

    def initialize(manager)
      super('Point Cloud Overlay')
      @manager = manager
      @render_states = {}
      @diagnostic_lines = []
      @last_diagnostic_update = -Float::INFINITY
      @color_draw_context = { color_pool: [], color_buffer: [] }
    end

    def draw(view)
      Threading.guard(:ui, message: 'ViewerOverlay#draw')
      clouds = Array(@manager.clouds)
      apply_render_throttle(clouds)
      draw_context = ensure_color_draw_context
      @manager.draw(view, draw_context)
      update_diagnostics(view)
      render_diagnostics(view)
      trim_color_draw_context
      cleanup_render_states(clouds)
    end

    def on_added(view)
      view.invalidate
    end

    def transparency
      :opaque
    end

    private

    def apply_render_throttle(clouds)
      return unless clouds

      clouds.each do |cloud|
        next unless cloud
        next unless cloud.respond_to?(:visible?) && cloud.visible?
        next unless cloud.respond_to?(:apply_render_constraints!)

        throttled = throttle_required?(cloud)
        next unless throttled

        update_render_state(cloud)
      end
    end

    def throttle_required?(cloud)
      startup_phase?(cloud) || @render_states.key?(cloud)
    end

    def update_render_state(cloud)
      state = (@render_states[cloud] ||= {})

      target_density = fetch_density(cloud)
      target_limit = fetch_target_limit(cloud)
      base_limit = fetch_base_limit(cloud)
      effective_target_limit = base_limit.positive? ? [target_limit, base_limit].min : target_limit

      startup = startup_phase?(cloud)

      density = nil
      limit = nil

      if startup
        density = [target_density, STARTUP_DENSITY].min
        limit = effective_target_limit
        state[:phase] = :startup
        state[:last_density] = density
        state[:last_limit] = limit
        state[:ramp_started_at] = nil
      else
        start_density = state[:last_density] || [target_density, STARTUP_DENSITY].min
        start_limit = state[:last_limit] || effective_target_limit
        ramp_started_at = state[:ramp_started_at] ||= monotonic_time
        progress = ((monotonic_time - ramp_started_at) / RAMP_DURATION.to_f)
        progress = [[progress, 0.0].max, 1.0].min
        density = start_density + ((target_density - start_density) * progress)
        limit = start_limit + ((effective_target_limit - start_limit) * progress)
        state[:phase] = progress >= 1.0 ? :stable : :ramp
        state[:last_density] = density
        state[:last_limit] = limit
      end

      if render_constraints_completed?(state, density, target_density, limit, effective_target_limit)
        cloud.clear_render_constraints!
        @render_states.delete(cloud)
        return
      end

      density = [[density, 1.0].min, 0.01].max
      limit_value = limit.round
      limit_value = 0 if limit_value.negative?
      cloud.apply_render_constraints!(density: density, max_display_points: limit_value)
    end

    def fetch_density(cloud)
      if cloud.respond_to?(:render_density)
        cloud.render_density
      elsif cloud.respond_to?(:density)
        cloud.density
      else
        1.0
      end
    end

    def fetch_target_limit(cloud)
      if cloud.respond_to?(:max_display_points)
        cloud.max_display_points.to_i
      elsif cloud.respond_to?(:render_max_display_points)
        cloud.render_max_display_points.to_i
      else
        0
      end
    end

    def fetch_base_limit(cloud)
      if cloud.respond_to?(:lod0_point_count)
        cloud.lod0_point_count.to_i
      else
        0
      end
    end

    def render_constraints_completed?(state, density, target_density, limit, target_limit)
      return false unless state
      return false if state[:phase] == :startup

      density_close = (density - target_density).abs <= DENSITY_EPSILON
      limit_close = (limit.to_i - target_limit.to_i).abs <= MAX_LIMIT_EPSILON
      density_close && limit_close
    end

    def cleanup_render_states(clouds)
      active = clouds ? clouds.compact : []
      @render_states.keys.each do |cloud|
        next if active.include?(cloud)

        begin
          cloud.clear_render_constraints! if cloud.respond_to?(:clear_render_constraints!)
        rescue StandardError
          # ignore cleanup failures
        end
        @render_states.delete(cloud)
      end
    end

    def ensure_color_draw_context
      context = (@color_draw_context ||= { color_pool: [], color_buffer: [] })
      context[:color_pool] ||= []
      context[:color_buffer] ||= []
      context
    end

    def trim_color_draw_context
      context = @color_draw_context
      return unless context

      pool = context[:color_pool]
      buffer = context[:color_buffer]
      max_capacity = DRAW_CHUNK

      if pool && pool.length > max_capacity
        pool.slice!(max_capacity, pool.length - max_capacity)
      end

      if buffer && buffer.length > max_capacity
        buffer.slice!(max_capacity, buffer.length - max_capacity)
      end
    end

    def update_diagnostics(view)
      now = monotonic_time
      return if (now - @last_diagnostic_update) < DIAGNOSTIC_UPDATE_INTERVAL

      @last_diagnostic_update = now
      lines = []

      fps_text = sample_fps(view)
      lines << "FPS: #{fps_text}" if fps_text

      active_cloud = @manager.active_cloud
      if active_cloud && active_cloud.visible?
        visible = active_cloud.respond_to?(:last_visible_point_count) ?
                    active_cloud.last_visible_point_count.to_i : 0
        lod = active_cloud.respond_to?(:current_lod_level) ? active_cloud.current_lod_level : nil
        sampling_ms = if active_cloud.respond_to?(:last_sampling_duration)
                        duration = active_cloud.last_sampling_duration
                        duration ? duration * 1000.0 : nil
                      end

        lines << format('Visible: %s pts | LOD: %s',
                         format_point_count(visible),
                         format_lod(lod))
        lines << format('Sample: %s ms', sampling_ms ? format('%.1f', sampling_ms) : 'n/a')
      else
        lines << 'Облако не выбрано'
      end

      @diagnostic_lines = lines
    rescue StandardError => e
      Logger.debug { "Overlay diagnostics update failed: #{e.message}" }
    end

    def render_diagnostics(view)
      lines = @diagnostic_lines
      return unless lines && !lines.empty?

      base_x = 12
      base_y = 24
      line_height = 16

      lines.each_with_index do |text, index|
        view.draw_text([base_x, base_y + (index * line_height)], text)
      end
    rescue StandardError => e
      Logger.debug { "Overlay diagnostics render failed: #{e.message}" }
    end

    def sample_fps(view)
      return nil unless view

      if view.respond_to?(:average_refresh_time)
        refresh = view.average_refresh_time
        return nil unless refresh && refresh.positive?

        format('%.2f', 1.0 / refresh)
      elsif view.respond_to?(:fps)
        fps = view.fps
        fps_value = fps.respond_to?(:to_f) ? fps.to_f : nil
        fps_value ? format('%.2f', fps_value) : nil
      end
    rescue StandardError
      nil
    end

    def format_point_count(value)
      count = value.to_i
      if count >= 1_000_000
        formatted = format('%.1fM', count / 1_000_000.0)
        formatted.sub(/\.0M\z/, 'M')
      elsif count >= 1_000
        formatted = format('%.1fK', count / 1_000.0)
        formatted.sub(/\.0K\z/, 'K')
      else
        count.to_s
      end
    end

    def format_lod(value)
      return 'n/a' unless value

      format('%.2f', value.to_f)
    rescue StandardError
      'n/a'
    end

    def startup_phase?(cloud)
      pending = if cloud.respond_to?(:render_cache_preparation_pending?)
                  cloud.render_cache_preparation_pending?
                else
                  false
                end

      if !pending && cloud.respond_to?(:background_build_pending?)
        pending = cloud.background_build_pending?
      end

      pending
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue NameError, Errno::EINVAL
      Process.clock_gettime(:float_second)
    rescue NameError, ArgumentError, Errno::EINVAL
      Process.clock_gettime(Process::CLOCK_REALTIME)
    rescue NameError, Errno::EINVAL
      Time.now.to_f
    end
  end
end
