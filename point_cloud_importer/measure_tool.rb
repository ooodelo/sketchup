# frozen_string_literal: true

module PointCloudImporter
  # Interactive tool for measuring distances between points in a cloud.
  class MeasureTool
    STATE_FIRST = 0
    STATE_SECOND = 1

    def initialize(manager)
      @manager = manager
      reset!
    end

    def activate
      reset!
      Sketchup.status_text = 'Кликните по первой точке облака.'
    end

    def deactivate(view)
      reset!
      view.invalidate if view
      Sketchup.status_text = ''
    end

    def onLButtonDown(flags, x, y, view)
      point = @manager.pick_point(view, x, y)
      return unless point

      case @state
      when STATE_FIRST
        @first_point = point
        @state = STATE_SECOND
        Sketchup.status_text = 'Выберите вторую точку.'
      when STATE_SECOND
        @second_point = point
        @state = STATE_FIRST
        display_measurement(view)
        Sketchup.status_text = 'Измерение завершено. Выберите новую первую точку.'
      end
      view.invalidate
    end

    def draw(view)
      return unless @first_point

      view.drawing_color = Sketchup::Color.new(255, 128, 0)
      view.line_width = 2

      if @second_point
        view.draw_polyline(@first_point, @second_point)
        draw_dimension(view)
      else
        view.draw_points([@first_point], size: 6, style: Sketchup::View::DRAW_POINTS_ROUND)
      end
    end

    private

    def reset!
      @state = STATE_FIRST
      @first_point = nil
      @second_point = nil
    end

    def display_measurement(view)
      return unless @first_point && @second_point

      distance = @first_point.distance(@second_point)
      UI.messagebox("Расстояние: #{Sketchup.format_length(distance)}")
    end

    def draw_dimension(view)
      mid_point = Geom.linear_combination(0.5, @first_point, 0.5, @second_point)
      screen = view.screen_coords(mid_point)
      distance = @first_point.distance(@second_point)
      label = Sketchup.format_length(distance)
      view.draw_text(screen, label, size: 14, color: Sketchup::Color.new(255, 255, 255))
    end
  end
end
