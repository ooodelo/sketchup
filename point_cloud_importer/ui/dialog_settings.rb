# encoding: utf-8
# frozen_string_literal: true

require_relative '../point_cloud'

module PointCloudImporter
  module UI
    # Normalises data coming from HtmlDialog callbacks and forwards the
    # resulting values to a render target (tool or cloud).
    class DialogSettings
      MEMORY_KEYS = %i[memory_limit_bytes memory_limit memory_limit_mb].freeze

      def initialize(target, manager)
        @target = target
        @manager = manager
      end

      def apply(payload)
        data = normalize_payload(payload)
        return if data.empty?

        apply_render_settings(data)
        apply_color_settings(data)
        apply_memory_limit(data)
        invalidate_view
      end

      private

      def normalize_payload(payload)
        hash = case payload
               when Hash then payload.dup
               when Array then payload[0].is_a?(Hash) ? payload[0].dup : {}
               else
                 {}
               end

        normalised = {}
        extract_numeric_values(hash, normalised)
        extract_color_values(hash, normalised)
        extract_memory_value(hash, normalised)
        normalised.delete_if { |_key, value| value.nil? }
        normalised
      end

      def extract_numeric_values(hash, output)
        value = extract_value(hash, :point_size)
        if value
          candidate = value.to_i
          candidate = candidate.clamp(1, 10)
          output[:point_size] = candidate
        end

        density_value = extract_value(hash, :render_density, :display_density, :density)
        if density_value
          candidate = density_value.to_f
          candidate = 1.0 unless candidate.finite?
          candidate = candidate.clamp(0.01, 1.0)
          output[:density] = candidate
        end

        budget_value = extract_value(hash, :max_display_points, :render_budget)
        if budget_value
          candidate = sanitize_integer(budget_value)
          output[:max_display_points] = candidate if candidate
        end
      end

      def extract_color_values(hash, output)
        mode_value = extract_value(hash, :color_mode)
        if mode_value
          output[:color_mode] = normalize_color_mode(mode_value)
        end

        gradient_value = extract_value(hash, :color_gradient)
        if gradient_value
          output[:color_gradient] = normalize_color_gradient(gradient_value)
        end

        single_color = extract_value(hash, :single_color)
        output[:single_color] = single_color if single_color
      end

      def extract_memory_value(hash, output)
        memory_value = extract_value(hash, *MEMORY_KEYS)
        return unless memory_value

        bytes = normalize_memory_limit(memory_value)
        output[:memory_limit_bytes] = bytes if bytes
      end

      def apply_render_settings(data)
        return unless @target

        if data.key?(:density)
          set_if_available(:density=, data[:density])
          set_if_available(:render_density=, data[:density])
        end

        if data.key?(:max_display_points)
          set_if_available(:max_display_points=, data[:max_display_points])
          set_if_available(:render_budget=, data[:max_display_points])
        end

        set_if_available(:point_size=, data[:point_size]) if data.key?(:point_size)
      end

      def apply_color_settings(data)
        return unless @target

        set_if_available(:color_mode=, data[:color_mode]) if data.key?(:color_mode)
        set_if_available(:color_gradient=, data[:color_gradient]) if data.key?(:color_gradient)
        set_if_available(:single_color=, data[:single_color]) if data.key?(:single_color)
      end

      def apply_memory_limit(data)
        return unless @target
        value = data[:memory_limit_bytes]
        return unless value

        if @target.respond_to?(:memory_limit_bytes=)
          @target.memory_limit_bytes = value
          return
        end

        if @target.respond_to?(:store)
          store = @target.store
          store.memory_limit_bytes = value if store && store.respond_to?(:memory_limit_bytes=)
        elsif @target.respond_to?(:storage)
          storage = @target.storage
          storage.memory_limit_bytes = value if storage && storage.respond_to?(:memory_limit_bytes=)
        end
      end

      def invalidate_view
        manager = if @target.respond_to?(:manager)
                    @target.manager
                  else
                    @manager
                  end
        view = manager.respond_to?(:view) ? manager.view : nil
        view&.invalidate if view.respond_to?(:invalidate)
      rescue StandardError
        nil
      end

      def set_if_available(method, value)
        return unless value
        return unless @target.respond_to?(method)

        @target.public_send(method, value)
      rescue StandardError
        nil
      end

      def extract_value(hash, *keys)
        keys.each do |key|
          next unless hash.key?(key) || hash.key?(key.to_s)

          value = hash[key]
          value = hash[key.to_s] if value.nil?
          value = value.first if value.is_a?(Array) && !value.empty?
          return value
        end
        nil
      end

      def sanitize_integer(value)
        candidate = case value
                    when Array then value.first
                    else value
                    end
        candidate = candidate.to_i
        candidate >= 0 ? candidate : 0
      rescue StandardError
        nil
      end

      def normalize_color_mode(value)
        return nil unless value

        key = value.to_s.strip.downcase.to_sym
        if PointCloud::COLOR_MODE_LOOKUP && PointCloud::COLOR_MODE_LOOKUP.key?(key)
          key
        else
          nil
        end
      rescue StandardError
        nil
      end

      def normalize_color_gradient(value)
        key = value.to_s.strip.downcase.to_sym
        if PointCloud::COLOR_GRADIENTS && PointCloud::COLOR_GRADIENTS.key?(key)
          key
        else
          nil
        end
      rescue StandardError
        nil
      end

      def normalize_memory_limit(value)
        number = case value
                 when Array then value.first
                 else value
                 end
        return nil if number.nil?

        if number.to_s.strip.empty?
          return 0
        end

        numeric = number.to_f
        numeric = 0.0 unless numeric.finite?
        if number.to_s.strip =~ /mb\z/i
          (numeric * 1024 * 1024).to_i
        elsif numeric <= 0
          0
        else
          numeric.to_i
        end
      rescue StandardError
        nil
      end
    end
  end
end

