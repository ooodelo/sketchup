# frozen_string_literal: true

module PointCloudImporter
  # Overlay responsible for rendering active point clouds efficiently.
  class ViewerOverlay < Sketchup::Overlay
    DRAW_CHUNK = 250_000
    STARTUP_DENSITY = 0.2
    RAMP_DURATION = 1.5
    MAX_LIMIT_EPSILON = 1
    DENSITY_EPSILON = 0.01

    def initialize(manager)
      super('Point Cloud Overlay')
      @manager = manager
      @render_states = {}
    end

    def draw(view)
      clouds = Array(@manager.clouds)
      apply_render_throttle(clouds)
      @manager.draw(view)
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
