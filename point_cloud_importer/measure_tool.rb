# frozen_string_literal: true

module PointCloudImporter
  # Interactive tool for measuring distances between points in a cloud.
  class MeasureTool
    STATE_FIRST = 0
    STATE_SECOND = 1

    def initialize(manager)
      @manager = manager
      @measurements = []
      reset!
    end

    def activate
      reset!
      @measurements.clear
      Sketchup.status_text = 'Кликните по первой точке облака.'
    end

    def deactivate(view)
      reset!
      @measurements.clear
      view.invalidate if view
      Sketchup.status_text = ''
    end

    def onLButtonDown(flags, x, y, view)
      case @state
      when STATE_FIRST
        point = active_point(view, x, y)
        return unless point

        @first_point = point
        @second_point = nil
        @state = STATE_SECOND
        Sketchup.status_text = 'Выберите вторую точку.'
        view.invalidate
      when STATE_SECOND
        # Сбросить предыдущую точку, чтобы убрать старое превью измерения,
        # пока пользователь выбирает новую точку.
        @second_point = nil
        view.invalidate

        point = active_point(view, x, y)
        return unless point

        @second_point = point
        finalize_measurement(view)
        Sketchup.status_text = 'Измерение завершено. Выберите новую первую точку.'
        view.invalidate
      end
    end

    def onMouseMove(_flags, x, y, view)
      previous_hover = @hover_point
      previous_cursor = @cursor_position

      @cursor_position = Geom::Point3d.new(x, y, 0)
      @hover_point = nil
      @hover_screen_point = nil

      point = @manager.pick_point(view, x, y)
      if point
        screen_point = view.screen_coords(point)
        distance = Math.hypot(screen_point.x - x, screen_point.y - y)

        if distance < 20
          @hover_point = point
          @hover_screen_point = screen_point
        end
      end

      hover_changed = previous_hover != @hover_point
      cursor_changed = !previous_cursor || previous_cursor.x != x || previous_cursor.y != y

      view.invalidate if hover_changed || (cursor_changed && @hover_point)
    end

    def draw(view)
      draw_hover_feedback(view)

      view.drawing_color = Sketchup::Color.new(255, 128, 0)
      view.line_width = 2

      @measurements.each do |measurement|
        draw_measurement(view, measurement[:start], measurement[:finish], measurement[:label])
      end

      return unless @first_point

      if @second_point
        draw_measurement(view, @first_point, @second_point, formatted_distance(@first_point, @second_point))
      else
        style = PointCloud.style_constant(:round)
        options = { size: 6 }
        options[:style] = style if style
        view.draw_points([@first_point], **options)
      end
    end

    private

    def reset!
      @state = STATE_FIRST
      @first_point = nil
      @second_point = nil
      @hover_point = nil
      @hover_screen_point = nil
      @cursor_position = nil
    end

    def finalize_measurement(view)
      return unless @first_point && @second_point

      label = formatted_distance(@first_point, @second_point)
      @measurements << { start: @first_point, finish: @second_point, label: label }
      UI.messagebox("Расстояние: #{label}")
      reset!
      view.invalidate if view
    end

    def draw_measurement(view, start_point, end_point, label)
      view.draw_polyline(start_point, end_point)
      draw_dimension(view, start_point, end_point, label)
    end

    def draw_dimension(view, start_point, end_point, label)
      mid_point = Geom.linear_combination(0.5, start_point, 0.5, end_point)
      screen = view.screen_coords(mid_point)
      view.draw_text(screen, label, size: 14, color: Sketchup::Color.new(255, 255, 255))
    end

    def formatted_distance(first_point, second_point)
      Sketchup.format_length(first_point.distance(second_point))
    end

    def active_point(view, x, y)
      @hover_point || @manager.pick_point(view, x, y)
    end

    def draw_hover_feedback(view)
      return unless @hover_point

      style = PointCloud.style_constant(:round)
      options = { size: 8, color: Sketchup::Color.new(255, 255, 0) }
      options[:style] = style if style
      view.draw_points([@hover_point], **options)

      return unless @cursor_position && @hover_screen_point

      view.line_width = 1
      view.drawing_color = Sketchup::Color.new(255, 255, 0)
      cursor_2d = Geom::Point3d.new(@cursor_position.x, @cursor_position.y, 0)
      hover_2d = Geom::Point3d.new(@hover_screen_point.x, @hover_screen_point.y, 0)
      view.draw2d(GL_LINE_STRIP, [cursor_2d, hover_2d])
    end
  end
end
