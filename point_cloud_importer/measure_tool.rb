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
      point = @manager.pick_point(view, x, y)
      return unless point

      case @state
      when STATE_FIRST
        @first_point = point
        @second_point = nil
        @state = STATE_SECOND
        Sketchup.status_text = 'Выберите вторую точку.'
      when STATE_SECOND
        @second_point = point
        finalize_measurement(view)
        Sketchup.status_text = 'Измерение завершено. Выберите новую первую точку.'
      end
      view.invalidate
    end

    def draw(view)
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
  end
end
