# frozen_string_literal: true

require_relative 'settings'

module PointCloudImporter
  # Overlay responsible for rendering active point clouds efficiently.
  class ViewerOverlay < Sketchup::Overlay
    def initialize(manager)
      super('Point Cloud Overlay')
      @manager = manager
      @gl_renderer = OpenGLRenderer.build(manager)
    end

    def draw(view)
      if @gl_renderer&.active?
        @manager.draw(view) unless @gl_renderer.last_render_success?
      else
        @manager.draw(view)
      end
    end

    def render_overlay(view, render_info)
      return unless @gl_renderer&.active?

      rendered = @gl_renderer.render(view, render_info)
      @manager.draw(view) unless rendered
    end

    def on_added(view)
      view.invalidate
    end

    def on_removed(_view)
      dispose!
    end

    def transparency
      :opaque
    end

    def cloud_removed(cloud)
      @gl_renderer&.cloud_removed(cloud)
    end

    def dispose!
      @gl_renderer&.dispose!
    end

    # Handles OpenGL/VBO rendering when SketchUp provides a GPU overlay context.
    class OpenGLRenderer
      BufferRecord = Struct.new(:buffer, :count, :revision)

      def self.build(manager)
        renderer = new(manager)
        renderer.active? ? renderer : nil
      end

      def initialize(manager)
        @manager = manager
        @buffers = {}
        @context = nil
        @settings = Settings.instance
        @active = api_available? && @settings[:prefer_hardware_overlay] != false
        @last_render_success = false
      end

      def active?
        @active
      end

      def last_render_success?
        @last_render_success
      end

      def render(view, render_info)
        return false unless active?

        context = extract_context(render_info)
        return false unless context

        @context = context
        rendered_any = false
        @manager.clouds.each do |cloud|
          next unless cloud.visible?

          rendered_any ||= cloud.draw(view, renderer: self, fallback: false)
        end
        @last_render_success = rendered_any
        rendered_any
      rescue StandardError => error
        report_failure(error)
        disable!
        false
      ensure
        @context = nil
      end

      def draw_cloud(cloud, view)
        context = @context
        return false unless context

        record = ensure_buffer(context, cloud)
        return false unless record && record.buffer && record.count.to_i.positive?

        update_view(context, view)
        draw_buffer(context, record, cloud)
      rescue StandardError => error
        report_failure(error)
        disable!
        false
      end

      def cloud_removed(cloud)
        record = @buffers.delete(cloud.object_id)
        release_buffer(record)
      end

      def dispose!
        @buffers.each_value { |record| release_buffer(record) }
        @buffers.clear
      end

      private

      def api_available?
        defined?(Sketchup::Overlay) &&
          (defined?(Sketchup::Overlay::OpenGL) || defined?(Sketchup::Overlay::GL))
      rescue StandardError
        false
      end

      def extract_context(render_info)
        return nil unless render_info

        if render_info.respond_to?(:opengl_context)
          render_info.opengl_context
        elsif render_info.respond_to?(:[])
          render_info[:opengl_context] || render_info['opengl_context'] ||
            render_info[:context] || render_info['context']
        elsif render_info.respond_to?(:context)
          render_info.context
        else
          render_info
        end
      rescue StandardError
        nil
      end

      def ensure_buffer(context, cloud)
        record = (@buffers[cloud.object_id] ||= BufferRecord.new)
        revision = cloud.display_revision

        if record.buffer.nil? || record.revision != revision
          release_buffer(record) if record.buffer
          buffer = create_buffer(context, cloud)
          return unless buffer

          unless upload_buffer(buffer, cloud)
            release_buffer(BufferRecord.new(buffer, 0, revision))
            return
          end

          record.buffer = buffer
          record.count = cloud.display_points.length
          record.revision = revision
        end

        record
      end

      def create_buffer(context, cloud)
        layout = { position: 3 }
        layout[:color] = 4 if cloud.display_colors

        if context.respond_to?(:create_vertex_buffer)
          begin
            context.create_vertex_buffer(layout: layout)
          rescue ArgumentError
            context.create_vertex_buffer(layout)
          end
        elsif context.respond_to?(:create_vbo)
          context.create_vbo(layout)
        end
      rescue StandardError
        nil
      end

      def upload_buffer(buffer, cloud)
        points_data = flatten_points(cloud.display_points)
        return false if points_data.empty?

        return false unless upload_attribute(buffer, :position, points_data)

        colors = cloud.display_colors
        if colors && !colors.empty?
          colors_data = flatten_colors(colors)
          return false unless upload_attribute(buffer, :color, colors_data)
        end

        true
      end

      def upload_attribute(buffer, name, data)
        return false if data.empty?

        if buffer.respond_to?(:upload_attribute)
          buffer.upload_attribute(name, data)
          true
        elsif buffer.respond_to?(:assign)
          buffer.assign(name, data)
          true
        elsif buffer.respond_to?(:set_attribute)
          buffer.set_attribute(name, data)
          true
        elsif buffer.respond_to?("#{name}=")
          buffer.public_send("#{name}=", data)
          true
        else
          false
        end
      rescue StandardError
        false
      end

      def update_view(context, view)
        if context.respond_to?(:set_view)
          context.set_view(view)
        elsif context.respond_to?(:update_view)
          context.update_view(view)
        elsif context.respond_to?(:set_matrices)
          context.set_matrices(view)
        end
      rescue StandardError
        # Ignore matrix upload failures and let fallback take over on next frame.
      end

      def draw_buffer(context, record, cloud)
        style_value = cloud.point_style_constant || cloud.point_style_symbol

        if context.respond_to?(:draw_points)
          options = {
            buffer: record.buffer,
            count: record.count,
            size: cloud.point_size,
            point_size: cloud.point_size
          }
          options[:style] = style_value if style_value
          options.delete_if { |_key, value| value.nil? }

          begin
            context.draw_points(**options)
            return true
          rescue ArgumentError
            # Try legacy positional signatures before giving up.
            begin
              context.draw_points(record.buffer, record.count, cloud.point_size)
              return true
            rescue ArgumentError
              if style_value
                begin
                  context.draw_points(record.buffer, record.count, cloud.point_size, style_value)
                  return true
                rescue ArgumentError
                  # Give up and fall through to generic fallback.
                end
              end
            end
          end
        end

        if context.respond_to?(:draw_arrays)
          context.draw_arrays(:points, record.buffer, record.count)
          true
        elsif record.buffer.respond_to?(:draw)
          record.buffer.draw
          true
        else
          false
        end
      rescue StandardError
        false
      end

      def release_buffer(record)
        return unless record&.buffer

        buffer = record.buffer
        if buffer.respond_to?(:release)
          buffer.release
        elsif buffer.respond_to?(:dispose)
          buffer.dispose
        end
        record.buffer = nil
        record.count = 0
        record.revision = nil
      rescue StandardError
        record.buffer = nil if record
      end

      def flatten_points(points)
        points.each_with_object([]) do |point, memo|
          memo << point.x.to_f << point.y.to_f << point.z.to_f
        end
      end

      def flatten_colors(colors)
        colors.each_with_object([]) do |color, memo|
          rgba =
            if color.respond_to?(:to_a)
              color.to_a
            elsif color.is_a?(Array)
              color
            elsif color.respond_to?(:red) && color.respond_to?(:green) && color.respond_to?(:blue)
              alpha = color.respond_to?(:alpha) ? color.alpha : 255
              [color.red, color.green, color.blue, alpha]
            else
              [255, 255, 255, 255]
            end
          r = rgba[0] || 255
          g = rgba[1] || 255
          b = rgba[2] || 255
          a = rgba[3] || 255
          memo << r.to_f << g.to_f << b.to_f << a.to_f
        end
      end

      def report_failure(error)
        warn("[PointCloudImporter] OpenGL renderer disabled: #{error.message}") if defined?(warn)
      end

      def disable!
        return unless @active

        @active = false
        @last_render_success = false
        begin
          @settings[:prefer_hardware_overlay] = false
          @settings.save!
        rescue StandardError
          # Best-effort persistence, ignore failures.
        end
        dispose!
      end
    end
  end
end
