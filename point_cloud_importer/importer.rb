# frozen_string_literal: true

require 'stringio'

require_relative 'settings'
require_relative 'point_cloud'
require_relative 'ply_parser'

module PointCloudImporter
  # High-level importer orchestrating parsing and creation of point clouds.
  class Importer
    Result = Struct.new(:cloud, :duration)

    def initialize(manager)
      @manager = manager
    end

    def import_from_dialog
      path = UI.openpanel('Выберите файл облака точек', nil, 'PLY (*.ply)|*.ply||')
      return unless path

      options = prompt_options
      return unless options

      import(path, options)
    end

    def import(path, options = {})
      start_time = Time.now
      parser = PlyParser.new(path, decimation: options[:decimation], progress_callback: method(:report_progress))
      points, colors, metadata = parser.parse
      raise ArgumentError, 'Файл не содержит точек для импорта.' if points.nil? || points.empty?

      name = File.basename(path, '.*')
      cloud = PointCloud.new(name: name, points: points, colors: colors, metadata: metadata)
      cloud.density = options[:display_density] if options[:display_density]
      cloud.point_size = options[:point_size] if options[:point_size]
      cloud.point_style = options[:point_style] if options[:point_style]
      cloud.max_display_points = options[:max_display_points] if options[:max_display_points]

      @manager.add_cloud(cloud)
      duration = Time.now - start_time
      UI.messagebox("Импорт завершен за #{format('%.2f', duration)} сек. Точек: #{points.length}")
      Result.new(cloud, duration)
    rescue PlyParser::UnsupportedFormat => e
      UI.messagebox("Формат не поддерживается: #{e.message}")
      nil
    rescue StandardError => e
      UI.messagebox("Ошибка импорта: #{e.message}")
      nil
    ensure
      reset_progress
    end

    STYLE_LABELS = {
      square: 'квадрат',
      round: 'круг',
      plus: 'крест'
    }.freeze

    private

    def prompt_options
      settings = Settings.instance
      prompts = ['Размер точки (1-10)', 'Стиль точки', 'Доля отображаемых точек (0.01-1.0)', 'Максимум точек в кадре (10000-8000000)']
      defaults = [
        settings[:point_size],
        display_name(settings[:point_style]),
        settings[:density],
        settings[:max_display_points]
      ]
      style_list = available_style_labels.join('|')
      lists = ['', style_list, '', '']
      input = UI.inputbox(prompts, defaults, lists, 'Настройки визуализации')
      return unless input

      point_size, style_label, density, max_points = input
      {
        point_size: point_size.to_i,
        point_style: resolve_style(style_label),
        display_density: density.to_f,
        max_display_points: max_points.to_i
      }
    end

    def report_progress(percent, message)
      UI.status_text = format('Импорт облака: %<percent>.1f%% — %<message>s', percent: percent * 100.0, message: message)
    end

    def reset_progress
      UI.status_text = ''
    end

    def display_name(style)
      style = style.to_sym rescue nil
      style = PointCloud.default_point_style unless PointCloud.available_point_style?(style)
      STYLE_LABELS.fetch(style, STYLE_LABELS.fetch(PointCloud.default_point_style, 'квадрат'))
    rescue StandardError
      STYLE_LABELS.fetch(PointCloud.default_point_style, 'квадрат')
    end

    def resolve_style(value)
      return value if value.is_a?(Symbol) && PointCloud.available_point_style?(value)

      normalized = value.to_s.downcase
      STYLE_LABELS.each do |symbol, label|
        next unless PointCloud.available_point_style?(symbol)

        return symbol if label == normalized
      end

      style = case normalized
              when 'square' then :square
              when 'round' then :round
              when 'plus' then :plus
              when 'крест' then :plus
              when 'круг' then :round
              else
                PointCloud.default_point_style
              end

      PointCloud.available_point_style?(style) ? style : PointCloud.default_point_style
    end

    def available_style_labels
      labels = PointCloud.available_point_styles.filter_map { |style| STYLE_LABELS[style] }
      return labels unless labels.empty?

      [STYLE_LABELS.fetch(PointCloud.default_point_style, 'квадрат')]
    end

  end
end
